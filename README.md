# 🔍 Crime Count Forecasting using Apache Spark

> A distributed crime prediction system that forecasts **weekly crime incidents per district** across Chicago to support data-driven public safety planning.

---

## 📌 Overview

Instead of classifying locations as high or low risk, this system predicts the **expected number of crime incidents per district** for future time periods using Apache Spark's distributed computing capabilities.

---

## 📂 Dataset

| Property | Details |
|----------|---------|
| **Name** | Crimes – 2001 to Present |
| **Source** | Chicago Open Data Portal |
| **Period** | 2021 – 2024 |
| **Records** | 971,865 |
| **File Size** | ~330 MB |
| **Format** | CSV |

🔗 [Download Raw Dataset](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

The dataset contains spatial, temporal, and categorical attributes such as district, crime type, location, and timestamps.

### Preprocessed Dataset
The preprocessed dataset was generated after applying all cleaning, reduction, and transformation steps. It contains aggregated weekly crime counts per district along with engineered features used for forecasting.

🔗 [Download Preprocessed Dataset](https://drive.google.com/drive/folders/1WQXSP-ZE23fDB3HHBddJOebZY1Yfe1zI?usp=share_link)

---

## ⚙️ Tech Stack

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Scala](https://img.shields.io/badge/Scala-DC322F?style=flat&logo=scala&logoColor=white)
![Spark SQL](https://img.shields.io/badge/Spark%20SQL-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Spark MLlib](https://img.shields.io/badge/Spark%20MLlib-E25A1C?style=flat&logo=apachespark&logoColor=white)

---

## 🔄 Data Processing Pipeline

### 1. 🧹 Data Cleaning
- Removed records with missing geographic information
- Verified valid date range (2021–2024)
- Checked for and removed duplicate records

| Stage | Rows |
|-------|------|
| Original dataset | 971,865 |
| After cleaning | 952,563 |

---

### 2. ✂️ Data Reduction
- Selected key features: `date`, `district`, `primary_type`
- Aggregated crime incidents **weekly per district**
- Pivoted crime types into numerical features

| Stage | Rows | Columns |
|-------|------|---------|
| Original dataset | 971,865 | 22 |
| After reduction | 4,667 | 33 |

---

### 3. 🔧 Data Transformation
- Engineered `month` and `week_no` for seasonal patterns
- Encoded `district` using `StringIndexer`
- Created lag feature `prev_week_total` to capture crime momentum

---

## 🎯 Project Goal

Build a **regression model** that forecasts weekly crime counts per district, enabling better resource allocation and proactive crime prevention.

---

## 👥 Team

This project was developed as part of the **IT462 Big Data Systems** course at King Saud University.

| Name |
|------|
| Raghad Fares Almutairi |
| — |
| — |
| — |
