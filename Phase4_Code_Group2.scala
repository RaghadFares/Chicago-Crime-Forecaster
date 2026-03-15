// ============================================================
// IT 462 – Big Data Systems
// Phase 4: SQL Operations in Apache Spark
// Chicago District-Level Crime Count Forecasting
// Semester 2, 1447H (January–May 2026)
// Group 2: Raghad Almutairi, Sarah Alruwayte,
//           Norah Alfaheed, Atheer Budie
// ============================================================

// ============================================
// STEP 1: Load Cleaned Dataset
// ============================================

val cleanedDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/Users/raghadfares/Desktop/BigData/clean_data.csv")

println(s"Cleaned dataset rows: ${cleanedDF.count()}")
println(s"Cleaned dataset columns: ${cleanedDF.columns.length}")

// ============================================
// STEP 2: Load Preprocessed Dataset
// ============================================

val preprocessedDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/Users/raghadfares/Desktop/BigData/preprocessed_Data.csv")

println(s"Preprocessed dataset rows: ${preprocessedDF.count()}")
println(s"Preprocessed dataset columns: ${preprocessedDF.columns.length}")

// ============================================
// STEP 3: Register as SQL Views
// ============================================

cleanedDF.createOrReplaceTempView("crimes_cleaned")
preprocessedDF.createOrReplaceTempView("crimes_weekly")

println("Both datasets registered as SQL views successfully.")
println("crimes_cleaned -> cleaned dataset (952,563 records)")
println("crimes_weekly  -> preprocessed weekly dataset (4,644 rows)")

// ============================================
// STEP 4: Print Schemas
// ============================================

println("--- Cleaned Dataset Schema ---")
cleanedDF.printSchema()

println("--- Preprocessed Dataset Schema ---")
preprocessedDF.printSchema()

// ============================================
// STEP 5: Print Sample Rows
// ============================================

println("--- Cleaned Dataset Sample (5 rows, key columns) ---")
cleanedDF.select(
  "id", "date", "primary_type", "district",
  "arrest", "domestic", "year"
).show(5, truncate = false)

println("--- Preprocessed Dataset Sample (5 rows, key columns) ---")
preprocessedDF.select(
  "district", "week_start", "month", "week_no",
  "THEFT", "BATTERY", "ROBBERY",
  "total_crime_count", "prev_week_total"
).show(5, truncate = false)

// ============================================
// QUERY 1: Monthly Crime Trend
// Question: What is the total and average weekly
// crime count per month across all districts?
// Dataset: crimes_weekly (preprocessed)
// Covers: GROUP BY, ORDER BY, aggregation
// ============================================

val query1 = spark.sql("""
  SELECT
    month,
    COUNT(*) AS total_weeks,
    SUM(total_crime_count) AS total_crimes,
    ROUND(AVG(total_crime_count), 2) AS avg_weekly_crimes,
    MAX(total_crime_count) AS max_weekly_crimes,
    MIN(total_crime_count) AS min_weekly_crimes
  FROM crimes_weekly
  GROUP BY month
  ORDER BY month
""")

query1.show(12, truncate = false)

// ============================================
// QUERY 2: Top 10 Highest Crime Weeks
// Question: What are the 10 highest crime weeks
// ever recorded and which district had them?
// Dataset: crimes_weekly (preprocessed)
// Covers: ORDER BY, LIMIT, window function (RANK)
// ============================================

val query2 = spark.sql("""
  SELECT
    district,
    week_start,
    month,
    week_no,
    total_crime_count,
    RANK() OVER (ORDER BY total_crime_count DESC) AS crime_rank
  FROM crimes_weekly
  ORDER BY total_crime_count DESC
  LIMIT 10
""")

query2.show(10, truncate = false)

// ============================================
// QUERY 3: Districts Above City Average Arrest Rate
// Question: Which districts have an arrest rate
// above the city-wide average?
// Dataset: crimes_cleaned
// Covers: CTE, subquery, GROUP BY
// ============================================

val query3 = spark.sql("""
  WITH district_arrests AS (
    SELECT
      district,
      COUNT(*) AS total_crimes,
      SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) AS total_arrests,
      ROUND(SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate
    FROM crimes_cleaned
    GROUP BY district
  ),
  city_average AS (
    SELECT ROUND(AVG(arrest_rate), 2) AS avg_arrest_rate
    FROM district_arrests
  )
  SELECT
    d.district,
    d.total_crimes,
    d.total_arrests,
    d.arrest_rate,
    c.avg_arrest_rate,
    CASE WHEN d.arrest_rate > c.avg_arrest_rate THEN 'ABOVE AVERAGE'
         ELSE 'BELOW AVERAGE' END AS performance
  FROM district_arrests d, city_average c
  ORDER BY d.arrest_rate DESC
""")

query3.show(25, truncate = false)

// ============================================
// QUERY 4: Statistical Summary of Key Crime Types
// Question: What is the average, variance and
// stddev of weekly THEFT, BATTERY and ROBBERY
// counts per district?
// Dataset: crimes_weekly (preprocessed)
// Covers: Statistical summaries (avg, variance, stddev)
// ============================================

val query4 = spark.sql("""
  SELECT
    district,
    ROUND(AVG(THEFT), 2)                    AS avg_theft,
    ROUND(STDDEV(THEFT), 2)                 AS stddev_theft,
    ROUND(AVG(BATTERY), 2)                  AS avg_battery,
    ROUND(STDDEV(BATTERY), 2)               AS stddev_battery,
    ROUND(AVG(ROBBERY), 2)                  AS avg_robbery,
    ROUND(STDDEV(ROBBERY), 2)               AS stddev_robbery,
    ROUND(AVG(total_crime_count), 2)        AS avg_total,
    ROUND(VARIANCE(total_crime_count), 2)   AS variance_total
  FROM crimes_weekly
  GROUP BY district
  ORDER BY avg_total DESC
  LIMIT 10
""")

query4.show(10, truncate = false)

// ============================================
// QUERY 5: Year-over-Year Crime Change per District
// Question: How did total yearly crime change
// per district between 2021 and 2024?
// Dataset: crimes_cleaned
// Covers: Window function (LAG), GROUP BY, ORDER BY
// ============================================

val query5 = spark.sql("""
  WITH yearly_totals AS (
    SELECT
      district,
      year,
      COUNT(*) AS yearly_crime_count
    FROM crimes_cleaned
    GROUP BY district, year
  ),
  yearly_with_change AS (
    SELECT
      district,
      year,
      yearly_crime_count,
      LAG(yearly_crime_count) OVER (
        PARTITION BY district ORDER BY year
      ) AS prev_year_count,
      ROUND(
        (yearly_crime_count - LAG(yearly_crime_count)
        OVER (PARTITION BY district ORDER BY year))
        * 100.0 / LAG(yearly_crime_count)
        OVER (PARTITION BY district ORDER BY year), 2
      ) AS pct_change
    FROM yearly_totals
  )
  SELECT
    district,
    year,
    yearly_crime_count,
    prev_year_count,
    pct_change,
    CASE
      WHEN pct_change > 0 THEN 'INCREASING'
      WHEN pct_change < 0 THEN 'DECREASING'
      ELSE 'NO CHANGE'
    END AS trend
  FROM yearly_with_change
  WHERE district IN (1, 4, 6, 8, 11, 12)
  ORDER BY district, year
""")

query5.show(30, truncate = false)

// ============================================
// QUERY 6: Peak Crime Hour Analysis
// Question: What hours of the day have the
// highest crime counts?
// Dataset: crimes_cleaned
// Covers: GROUP BY, ORDER BY, HOUR() function
// ============================================

val query6 = spark.sql("""
  SELECT
    HOUR(date) AS hour_of_day,
    COUNT(*) AS total_crimes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
  FROM crimes_cleaned
  GROUP BY HOUR(date)
  ORDER BY total_crimes DESC
""")

query6.show(24, truncate = false)

// ============================================
// QUERY 7: Domestic vs Non-Domestic Crime
// Question: What percentage of crimes are
// domestic-related per district?
// Dataset: crimes_cleaned
// Covers: GROUP BY, HAVING, conditional aggregation
// ============================================

val query7 = spark.sql("""
  SELECT
    district,
    COUNT(*) AS total_crimes,
    SUM(CASE WHEN domestic = true THEN 1 ELSE 0 END) AS domestic_crimes,
    SUM(CASE WHEN domestic = false THEN 1 ELSE 0 END) AS non_domestic_crimes,
    ROUND(SUM(CASE WHEN domestic = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS domestic_pct
  FROM crimes_cleaned
  GROUP BY district
  HAVING domestic_pct > 15
  ORDER BY domestic_pct DESC
""")

query7.show(25, truncate = false)

// ============================================
// QUERY 8: Districts with Consistently High
// Weekly Crime Above City Average
// Question: Which districts have an average
// weekly crime count above the overall average?
// Dataset: crimes_weekly (preprocessed)
// Covers: HAVING, subquery, GROUP BY
// ============================================

val query8 = spark.sql("""
  SELECT
    district,
    ROUND(AVG(total_crime_count), 2) AS avg_weekly_crimes,
    MAX(total_crime_count) AS max_weekly_crimes,
    MIN(total_crime_count) AS min_weekly_crimes,
    COUNT(*) AS total_weeks,
    ROUND((SELECT AVG(total_crime_count) FROM crimes_weekly), 2) AS city_avg
  FROM crimes_weekly
  GROUP BY district
  HAVING AVG(total_crime_count) > (
    SELECT AVG(total_crime_count) FROM crimes_weekly
  )
  ORDER BY avg_weekly_crimes DESC
""")

query8.show(25, truncate = false)
