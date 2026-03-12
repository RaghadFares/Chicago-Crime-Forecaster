// --- PART 0: DATA CLEANING ---

import org.apache.spark.sql.functions._
val path = "G:/.shortcut-targets-by-id/1mLF_qK_EJ6ane0HobmpHJNGJPDrEOfZQ/The data/chicago_crimes_2021_2024.csv"
val df= spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(path)
println("Rows: " + df.count())
println("Columns: " + df.columns.length)
val missLong =df.columns.map(c => (c, df.filter(col(c).isNull).count())).toSeq.toDF("column","missing_nulls")
missLong.orderBy(desc("missing_nulls")).show(50, false)
val duplicateCount = df.groupBy("id").count().filter(col("count") > 1).count()
println("Number of duplicated IDs = " + duplicateCount)
val outOfRange = df.filter( year(col("date")) < 2021 || year(col("date")) > 2024).count()
println("Out-of-range dates = " + outOfRange)
val invalidDistrict = df.filter(col("district") < 1 || col("district") > 31).count()
println("District values outside range (1–31) = " + invalidDistrict)
df.select("district").distinct().orderBy("district").show(50, false)
val dfClean = df.na.drop(Seq(
  "x_coordinate",
  "y_coordinate",
  "latitude",
  "longitude",
  "location",
  "location_description",
  "community_area",
  "ward"
))
val before = df.count()
println("before rows: " + (before ))
val after = dfClean.count()
println("after rows: " + (after ))
println("Removed rows: " + (before - after))

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

// Show schema after cleaning
println("Schema of the Original Dataset:")
dfClean.printSchema()

// --- PART 1: DATA REDUCTION ---
// Objective: Reduce volume/dimensionality while preserving behavioral insights.

// Step 1: Feature Selection
// Retaining only date, district, and primary_type to focus on spatial-temporal trends.
val dfSelected = dfClean.select("date", "district", "primary_type")

// Step 2: Pivoted Aggregation
// Aggregating 952,563 rows into weekly summaries per district.
// The pivot transforms categorical crime types into quantitative features.
val dfWeekly = dfSelected
  .withColumn("week_start", date_trunc("week", col("date")))
  .groupBy("district", "week_start")
  .pivot("primary_type") 
  .count()
  .na.fill(0) 

// Step 3: Target Variable Definition
// Calculating the sum of all crimes to serve as the regression target.
val crimeTypeCols = dfWeekly.columns.diff(Seq("district", "week_start"))
val dfWithTarget = dfWeekly.withColumn("total_crime_count", 
  crimeTypeCols.map(col).reduce(_ + _))


// --- PART 2: DATA TRANSFORMATION ---
// Objective: Prepare data for Spark MLlib and extract seasonal features.

// Step 4: Temporal Feature Engineering
// Extracting month and week numbers to capture annual crime seasonality.
val dfTransformed = dfWithTarget
  .withColumn("month", month(col("week_start")))
  .withColumn("week_no", weekofyear(col("week_start")))

// Step 5: Categorical Encoding
// Indexing the 'district' column as required for Spark ML algorithms.
val indexer = new StringIndexer()
  .setInputCol("district")
  .setOutputCol("district_indexed")

val dfIndexed = indexer.fit(dfTransformed).transform(dfTransformed)

// Step 6: Domain-Specific Feature Engineering (Lag Feature)
// Creating a 1-week lag to capture historical momentum in crime trends.
val windowSpec = Window.partitionBy("district").orderBy("week_start")

val dfFinal = dfIndexed
  .withColumn("prev_week_total", lag("total_crime_count", 1).over(windowSpec))
  .na.drop() // Removing the first record per district which has no lag.


// --- PART 3: OUTPUT & SNAPSHOT ---

// Step 7: Final Preprocessed Snapshot (10-20 rows)
// This fulfills the requirement for a snapshot of the final dataset.
println("Final Preprocessed Dataset Snapshot:")
dfFinal.select("district", "week_start", "THEFT", "total_crime_count", "prev_week_total")
       .show(20, truncate = false)

// Step 8: Exporting results to Mac Desktop.
val outputPath = "/Users/raghadfares/Desktop/BigData_Phase2/preprocessed_crimes_final"
dfFinal.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputPath)
