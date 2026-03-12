// ============================================================
// IT 462 – Big Data Systems | Phase 3: RDD Operations
// Project: Chicago District-Level Crime Count Forecasting
// Dataset: Chicago Crimes 2021–2024 (971,865 records)
// ============================================================

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.rdd.RDD

// ----------------------------------------------------------------
// SETUP: Load & clean data (reusing Phase 2 pipeline output)
// ----------------------------------------------------------------
// ----------------------------------------------------------------
// apply the same geographic cleaning rule
// used in Phase 2. Validation checks (duplicates, date range,
// district range) were performed in Phase 2 and are therefore
// not repeated here.
// ----------------------------------------------------------------
val path = "G:/.shortcut-targets-by-id/1mLF_qK_EJ6ane0HobmpHJNGJPDrEOfZQ/The data/chicago_crimes_2021_2024.csv"

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(path)

// Apply Phase 2 cleaning: drop rows with missing geographic fields
val dfClean = df.na.drop(Seq(
  "x_coordinate", "y_coordinate", "latitude", "longitude",
  "location", "location_description", "community_area", "ward"
))

// Convert to RDD of Row for RDD operations
val rawRDD = dfClean.rdd

println("=== Phase 3: RDD Operations ===")
println(s"Total clean records loaded: ${rawRDD.count()}")


// ================================================================
// ======================== TRANSFORMATIONS =======================
// ================================================================

// ----------------------------------------------------------------
// TRANSFORMATION 1: map
// Purpose: Extract (district, 1) key-value pairs from each record
//          to prepare for per-district crime counting.
// ----------------------------------------------------------------
println("\n--- T1: map – Extract (district, 1) pairs ---")

val districtOnePairs: RDD[(Int, Int)] = rawRDD.map { row =>
  val district = row.getAs[Int]("district")
  (district, 1)
}

// Preview the transformation (action: take)
println("Sample (district, 1) pairs:")
districtOnePairs.take(5).foreach(println)


// ----------------------------------------------------------------
// TRANSFORMATION 2: filter
// Purpose: Isolate only THEFT records — the most frequent crime
//          type (21.994% of dataset). Used to examine the dominant
//          crime pattern separately before building the full model.
// ----------------------------------------------------------------
println("\n--- T2: filter – Retain only THEFT records ---")

val theftRDD: RDD[org.apache.spark.sql.Row] = rawRDD.filter { row =>
  row.getAs[String]("primary_type") == "THEFT"
}

val theftCount = theftRDD.count()   // action used inside transformation block
println(s"Total THEFT records: $theftCount")



// ----------------------------------------------------------------
// TRANSFORMATION 3: reduceByKey
// Purpose: Compute total crime counts per district by summing
//          the (district, 1) pairs. Directly supports the regression
//          target: understanding which districts are highest-volume.
// ----------------------------------------------------------------
println("\n--- T3: reduceByKey – Total crimes per district ---")

val districtCrimeCount: RDD[(Int, Int)] = districtOnePairs.reduceByKey(_ + _)

println("Crime count per district (sample):")
districtCrimeCount.take(5).foreach { case (district, count) =>
  println(s"  District $district -> $count crimes")
}



// ----------------------------------------------------------------
// TRANSFORMATION 4: flatMap
// Purpose: Emit one (crimeType, 1) pair per record to build a
//          full frequency distribution of all ~30 primary crime
//          types. This reveals which crime categories dominate
//          and should carry the most predictive weight.
// ----------------------------------------------------------------
println("\n--- T4: flatMap – Crime type frequency distribution ---")

val crimeTypeFreq: RDD[(String, Int)] = rawRDD
  .flatMap { row =>
    val crimeType = row.getAs[String]("primary_type")
    Seq((crimeType, 1))   // one pair per record; flatMap opens the Seq
  }
  .reduceByKey(_ + _)

println("Crime type frequencies (sample):")
crimeTypeFreq.take(8).foreach { case (ctype, cnt) =>
  println(f"  $ctype%-35s -> $cnt%,d")
}


// ----------------------------------------------------------------
// TRANSFORMATION 5: groupByKey
// Purpose: Group all crime type occurrences by district to examine
//          the crime-type mix per district. This supports the
//          behavioral feature engineering rationale from Phase 2
//          (pivoted crime-type columns as predictors).
// ----------------------------------------------------------------
println("\n--- T5: groupByKey – Crime types grouped by district ---")

val districtCrimeTypes: RDD[(Int, Iterable[String])] = rawRDD
  .map { row =>
    val district  = row.getAs[Int]("district")
    val crimeType = row.getAs[String]("primary_type")
    (district, crimeType)
  }
  .groupByKey()

// Compute distinct crime type count per district
val distinctTypesPerDistrict: RDD[(Int, Int)] = districtCrimeTypes.map {
  case (district, types) => (district, types.toSet.size)
}

println("Distinct crime types per district (sample):")
distinctTypesPerDistrict.sortBy(_._1).take(5).foreach { case (d, n) =>
  println(s"  District $d -> $n distinct crime types")
}



// ----------------------------------------------------------------
// TRANSFORMATION 6: sortByKey
// Purpose: Sort districts in ascending order to produce a clean
//          ranked view of total crime volumes, supporting the
//          spatial component of the forecasting problem.
// ----------------------------------------------------------------
println("\n--- T6: sortByKey – Districts ranked by ID (ascending) ---")

val sortedDistrictCrimes: RDD[(Int, Int)] = districtCrimeCount.sortByKey(ascending = true)

println("Sorted district crime counts:")
sortedDistrictCrimes.take(10).foreach { case (district, count) =>
  println(f"  District $district%2d -> $count%,d crimes")
}



// ----------------------------------------------------------------
// TRANSFORMATION 7: join
// Purpose: Enrich district-level crime counts with the per-district
//          arrest counts to compute an arrest rate per district.
//          This derived metric helps reveal districts where crimes
//          are cleared quickly vs. those with low enforcement outcome.
// ----------------------------------------------------------------
println("\n--- T7: join – Enrich district counts with arrest counts ---")

// Arrest count per district
val districtArrestCount: RDD[(Int, Int)] = rawRDD
  .filter  { row => row.getAs[Boolean]("arrest") }
  .map     { row => (row.getAs[Int]("district"), 1) }
  .reduceByKey(_ + _)

// Join total crime counts with arrest counts
val districtEnriched: RDD[(Int, (Int, Int))] =
  districtCrimeCount.join(districtArrestCount)

println("Joined (district -> (total_crimes, arrests)) sample:")
districtEnriched.sortByKey().take(5).foreach { case (d, (total, arrests)) =>
  val rate = (arrests.toDouble / total * 100)
  println(f"  District $d%2d | Crimes: $total%,6d | Arrests: $arrests%,5d | Rate: $rate%.1f%%")
}



// ================================================================
// ============================ ACTIONS ===========================
// ================================================================

// ----------------------------------------------------------------
// ACTION 1: count
// Purpose: Confirm total number of records after cleaning and
//          verify scale of data requiring Spark's distributed engine.
// ----------------------------------------------------------------
println("\n--- A1: count – Total records after cleaning ---")

val totalRecords = rawRDD.count()
println(s"Total clean records: $totalRecords")



// ----------------------------------------------------------------
// ACTION 2: take
// Purpose: Inspect the top 5 highest-crime districts by retrieving
//          the sorted output. Confirms which districts should receive
//          the most attention in patrol resource allocation.
// ----------------------------------------------------------------
println("\n--- A2: take – Top 5 highest-crime districts ---")

val top5Districts = districtCrimeCount
  .sortBy(_._2, ascending = false)
  .take(5)

println("Top 5 districts by crime volume:")
top5Districts.foreach { case (district, count) =>
  println(f"  District $district%2d -> $count%,d crimes")
}



// ----------------------------------------------------------------
// ACTION 3: reduce
// Purpose: Compute the grand total of all crimes across the entire
//          city (2021–2024) using a distributed sum. This gives a
//          single city-wide baseline to contextualize district volumes.
// ----------------------------------------------------------------
println("\n--- A3: reduce – City-wide total crime count ---")

val cityTotal = districtCrimeCount
  .map(_._2)
  .reduce(_ + _)

println(s"City-wide total crimes (2021–2024): $cityTotal")



// ----------------------------------------------------------------
// ACTION 4: first
// Purpose: Retrieve the district with the highest weekly crime
//          volatility (std dev). High volatility districts are harder
//          to forecast and signal where the model needs more features.
// ----------------------------------------------------------------
println("\n--- A4: first – District with highest crime count (sorted desc) ---")

val highestCrimeDistrict = districtCrimeCount
  .sortBy(_._2, ascending = false)
  .first()

println(s"Highest-crime district: District ${highestCrimeDistrict._1} with ${highestCrimeDistrict._2} crimes")



// ----------------------------------------------------------------
// ACTION 5: foreach
// Purpose: Print a full ranked summary of all districts by crime
//          volume. Provides the spatial baseline needed to interpret
//          regression predictions in Phase 5.
// ----------------------------------------------------------------
println("\n--- A5: foreach – Full district ranking by crime volume ---")

println("All districts ranked by crime volume (descending):")
districtCrimeCount
  .sortBy(_._2, ascending = false)
  .collect()                        // small RDD: only ~31 districts
  .foreach { case (district, count) =>
    println(f"  District $district%2d -> $count%,d crimes")
  }



// ----------------------------------------------------------------
// ACTION 6: saveAsTextFile
// Purpose: Persist the enriched district-level summary (crime counts
//          + arrest rates) to disk as a readable text report for
//          inclusion in the final project submission.
// ----------------------------------------------------------------
println("\n--- A6: saveAsTextFile – Save district enrichment summary ---")

val outputPath = "/Users/raghadfares/Desktop/BigData_Phase3/district_crime_summary"

districtEnriched
  .sortByKey()
  .map { case (district, (total, arrests)) =>
    val rate = arrests.toDouble / total * 100
    f"District $district%2d | Total Crimes: $total%,6d | Arrests: $arrests%,5d | Arrest Rate: $rate%.2f%%"
  }
  .saveAsTextFile(outputPath)

println(s"District summary saved to: $outputPath")


// ================================================================
// BONUS: Year-over-Year Crime Trend per District
// Purpose: Emit (district, year, 1) triples and aggregate to reveal
//          whether crime is rising or falling per district — a key
//          insight for validating temporal forecasting patterns.
// ================================================================
println("\n--- BONUS: Year-over-Year crime trend per district ---")

val yearlyDistrictTrend: RDD[((Int, Int), Int)] = rawRDD
  .map { row =>
    val district = row.getAs[Int]("district")
    val year     = row.getAs[Int]("year")
    ((district, year), 1)
  }
  .reduceByKey(_ + _)

println("Yearly crime trend per district (District 11 example):")
yearlyDistrictTrend
  .filter { case ((district, _), _) => district == 11 }
  .sortBy { case ((_, year), _) => year }
  .collect()
  .foreach { case ((district, year), count) =>
    println(f"  District $district | Year $year | Crimes: $count%,d")
  }