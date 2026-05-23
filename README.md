# Chicago Crime Count Forecasting per District

**IT 462 – Big Data Systems | King Saud University | Spring 2026**

A regression-based forecasting system that predicts weekly crime incident counts per Chicago police district using Apache Spark with Scala. The system covers the full big data analytics pipeline: data preprocessing, RDD operations, Spark SQL analytics, and Spark MLlib machine learning.

---

## Team

| Name | Student ID |
|------|-----------|
| Raghad Fares Almutairi | 443200793 |
| Sarah Alruwayte | 444200758 |
| Norah Alfaheed | 444200779 |
| Atheer Budie | 444200894 |

**Instructor:** Dr. Afshan Jafri

---

## Project Overview

Chicago records nearly one million crime incidents annually across 23 active police districts. Traditional crime analysis is reactive rather than predictive, limiting the ability to pre-position resources efficiently. This project builds a quantitative forecasting system that predicts the total number of weekly crime incidents per district — enabling evidence-based patrol planning and resource allocation.

**Primary result:** Random Forest (4-feature reduced set) — RMSE = 28.54, MAE = 21.28, R² = 0.827, representing a 58.4% error reduction over the mean baseline.

---

## Dataset

| Property | Details |
|----------|---------|
| Name | Crimes – 2001 to Present |
| Source | City of Chicago Open Data Portal |
| Time Coverage | 2021 – 2024 |
| Raw Records | 971,865 incident-level records |
| Attributes | 22 original columns |
| Format | CSV |

[Raw Dataset](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

The preprocessed dataset (4,644 weekly rows, 38 columns) was generated after cleaning, aggregation, and feature engineering. It is available via Google Drive due to file size:

[Preprocessed Dataset – Google Drive](https://drive.google.com/drive/folders/1WQXSP-ZE23fDB3HHBddJOebZY1Yfe1zI?usp=share_link)

---

## Repository Structure

```
Chicago-Crime-Forecaster/
├── 01_DataPreprocessing.scala     # Cleaning, aggregation, feature engineering
├── 02_RDDOperations.scala         # 7 transformations + 6 actions on incident-level RDD
├── 03_SQLOperations.scala         # 8 SQL queries on two registered Spark views
├── 04_MachineLearning.scala       # 3 models x 2 feature sets + evaluation
├── FinalReport_BigData.pdf        # Full written report
├── Presentation.pdf               # Project presentation slides
└── README.md
```

---

## Environment Requirements

| Component | Version |
|-----------|---------|
| Apache Spark | 4.1.1 |
| Scala | 2.13.17 |
| Java | OpenJDK 21 |

---

## How to Run

Start the Spark shell:

```bash
spark-shell
```

Run each file in order. Each phase depends on the output of the previous one.

```scala
// Step 1 – Preprocessing (generates preprocessed_dataset.csv)
:load /path/to/01_DataPreprocessing.scala

// Step 2 – RDD Operations
:load /path/to/02_RDDOperations.scala

// Step 3 – SQL Operations
:load /path/to/03_SQLOperations.scala

// Step 4 – Machine Learning
:load /path/to/04_MachineLearning.scala
```

Replace `/path/to/` with the actual path to the files on your machine.

**Note:** Spark writes output as folders containing a `part-00000` file. Rename each:
- `results/rdd_output/part-00000` → `rdd_output.txt`
- `results/ml_metrics/part-00000` → `ml_metrics.txt`
- `results/sql_results/q1_.../part-00000` → `q1.csv` (repeat for q2–q8)

---

## Pipeline Summary

**Preprocessing**
- Dropped 19,302 rows with missing geographic fields (952,563 clean records retained)
- Feature selection reduced 22 columns to 3 (date, district, primary_type)
- Weekly aggregation produced 4,644 rows and 33 columns
- Feature engineering added month, week_no, district_indexed, and prev_week_total (lag feature)

**RDD Operations**
- 7 transformations: map, filter, reduceByKey, flatMap, groupByKey, sortByKey, join
- 6 actions: count, take, reduce, first, foreach+collect, saveAsTextFile
- Key finding: District 8 leads with 61,143 crimes; city-wide arrest rate is only 13.28%

**SQL Operations**
- 8 queries covering aggregation, window functions, CTEs, and subqueries
- Key finding: Crime peaks in July (avg 225.70/week); 11 of 23 districts exceed the city weekly average of 204.77

**Machine Learning**

| Model | RMSE | MAE | R² |
|-------|------|-----|----|
| Baseline (Mean) | 68.61 | 54.43 | 0.00 |
| Linear Regression – Full* | 0.09 | 0.06 | 0.9999 |
| Decision Tree – Full | 28.44 | 20.65 | 0.828 |
| Random Forest – Full | 21.07 | 14.83 | 0.906 |
| Linear Regression – Reduced | 29.05 | 20.98 | 0.821 |
| Decision Tree – Reduced | 29.19 | 21.35 | 0.819 |
| **Random Forest – Reduced** | **28.54** | **21.28** | **0.827** |

*LR Full result is an identity artifact (crime-type columns sum to the target). The reduced 4-feature set is the operationally valid evaluation.

**Top feature importances (Reduced Random Forest):**

| Feature | Importance |
|---------|-----------|
| prev_week_total | 82.9% |
| district_indexed | 12.7% |
| week_no | 3.0% |
| month | 1.3% |

---

## Tech Stack

- Apache Spark 4.1.1
- Scala 2.13.17
- Spark SQL
- Spark MLlib (LinearRegression, DecisionTreeRegressor, RandomForestRegressor, VectorAssembler, StandardScaler)
