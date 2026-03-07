
# Crime Count Forecasting using Apache Spark

## Overview

This project builds a **crime count forecasting system** using **Apache Spark** to analyze large-scale crime data from Chicago. Instead of classifying locations as high or low risk, the system predicts the **expected number of crime incidents per district** for future time periods.

The goal is to support **data-driven decision making** for public safety planning by identifying crime trends across districts and time.

---

## Dataset

**Name:** Crimes – 2001 to Present
**Source:** Chicago Open Data Portal
[https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

**Subset used in this project:**

* Time period: **2021 – 2024**
* Records: **971,865**
* File size: **~330 MB**
* Format: **CSV**

The dataset contains spatial, temporal, and categorical attributes such as district, crime type, location, and timestamps.

---
Preprocessed Dataset
The preprocessed dataset used for modeling was generated after applying the data cleaning, reduction, and transformation steps described in this project.
Due to the large size of the dataset, the processed file is hosted separately.
Download Link:
[https://drive.google.com/drive/folders/1WQXSP-ZE23fDB3HHBddJOebZY1Yfe1zI?usp=share_link]
The dataset contains aggregated weekly crime counts per district, along with engineered features used for forecasting.
---
## Technologies

* Apache Spark
* Spark SQL
* Spark MLlib
* Scala
* Distributed Data Processing

---

## Data Processing Pipeline

### 1. Data Cleaning

* Removed records with missing geographic information.
* Verified valid date range (2021–2024).
* Checked for duplicate records.

**Result**

| Stage            | Rows    |
| ---------------- | ------- |
| Original dataset | 971,865 |
| After cleaning   | 952,563 |

---

### 2. Data Reduction

To reduce dataset size and prepare it for forecasting:

* Selected key features: `date`, `district`, `primary_type`
* Aggregated crime incidents **weekly per district**
* Pivoted crime types into numerical features

**Result**

| Stage            | Rows    | Columns |
| ---------------- | ------- | ------- |
| Original dataset | 971,865 | 22      |
| After reduction  | 4,667   | 33      |

---

### 3. Data Transformation

Additional features were engineered:

* `month` and `week_no` (seasonal patterns)
* encoded `district` using StringIndexer
* created lag feature `prev_week_total`

These features help the model capture **temporal trends and crime momentum**.

---

## Project Goal

The final objective is to build a **regression model that forecasts weekly crime counts per district**, enabling better resource allocation and proactive crime prevention.

---

## Team

This project was developed as part of the **IT462 Big Data Systems course**.

**Team Members**

* Raghad Fares Almutairi
* [Add Team Member Name]
* [Add Team Member Name]
* [Add Team Member Name]



