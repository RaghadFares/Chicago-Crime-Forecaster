# Chicago Crime Data Analysis - Big Data Project
IT 462 - King Saud University

## Team Members
- Member 1: [Name] - [ID]
- Member 2: [Name] - [ID]
- Member 3: [Name] - [ID]
- Member 4: [Name] - [ID]

## Dataset Information
- **Name:** Chicago Crimes 2021-2024
- **Source:** City of Chicago Data Portal
- **Size:** ~2.5M rows, ~1 GB
- **URL:** https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2

## ⚠️ IMPORTANT: Download the Dataset

The full dataset is **NOT** included in this repository due to its size (1 GB).

### Option 1: Automatic Download (Recommended)
Run the download script:
```bash
python download_data.py
```

### Option 2: Manual Download
1. Go to: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2
2. Click "Filter" → Filter by Year: 2021-2024
3. Click "Export" → Choose "CSV"
4. Save as: `data/chicago_crimes_2021_2024.csv`

### Option 3: API Download
```bash
pip install sodapy pandas
python download_data.py
```

## Project Structure
```
BigDataProject/
├── code/           # Scala/Spark source code
├── data/           # Data files (gitignored, download separately)
├── results/        # Output files
└── docs/           # Documentation and reports
```

## Setup Instructions
1. Clone this repository
2. Download the dataset (see above)
3. Install Apache Spark 3.x with Scala 2.12
4. Run preprocessing: `spark-submit code/01_DataPreprocessing.scala`

## Dataset Schema
See `data/sample_data.csv` for structure (100 sample rows included)

Full dataset columns (22 total):
- id, case_number, date, block, iucr
- primary_type, description, location_description
- arrest, domestic, beat, district, ward
- community_area, fbi_code, x_coordinate, y_coordinate
- year, updated_on, latitude, longitude, location
