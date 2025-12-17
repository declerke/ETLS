# 🌍 World Bank ETL System (ETLS)

This repository contains a comprehensive ETL (Extract, Transform, Load) pipeline designed to process World Bank development indicators. The project is structured to handle end-to-end data workflows, from raw data extraction to PostgreSQL database storage, following a structured 40-question technical requirement.

## 📂 Repository Structure

The project is organized into a clean directory structure to separate concerns and maintain data integrity:

* **`cleaned/`**: Stores intermediate and final CSV files after data transformation and cleaning.
* **`data/`**: The landing zone for original, unprocessed World Bank datasets.
* **`logs/`**: Includes all log files for monitoring pipeline execution and debugging.
* **`etl.py`**: The main Python script that executes the full 40-step ETL pipeline.
* **`world_bank_etl_40_questions.ipynb`**: A Jupyter Notebook used for initial analysis, testing, and detailed documentation of the 40 questions.
* **`requirements.txt`**: Lists the dependencies required for the project.

## 🛠️ Technical Stack

* **Language:** Python 3.x
* **Libraries:** Pandas, NumPy, Psycopg2
* **Database:** PostgreSQL
* **Logging:** Python Logging Module

## 🚀 Getting Started

### 1. Installation
Ensure you have Python installed, then install the necessary dependencies:

```bash
pip install -r requirements.txt
```

## 📊 Pipeline Logic

The system follows a three-stage logic to ensure data quality and relational integrity:

### Extraction
- Identifies and loads raw CSV files from the `data/` directory.
- Performs initial checks on metadata and structural consistency.

### Transformation
- **Cleaning:** Renames columns for SQL compatibility and handles null values.
- **Normalization:** Reshapes data into a star-schema format, separating country-level metadata from time-series economic facts.
- **Validation:** Ensures data types are cast correctly for database insertion.

### Loading
- Automates the creation of relational tables (`countries` and `facts`) in PostgreSQL.
- Applies primary and foreign key constraints.
- Uses optimized bulk insertion for high-performance data loading.

## 📝 Logging and Monitoring

Monitoring is a core feature of the ETLS. Every execution generates a unique log file within the `logs/` directory named with the format `etl_YYYYMMDD_HHMMSS.log`.

The logs track:
- Pipeline start and end timestamps.
- Number of records successfully extracted and cleaned.
- Database connection status and transaction commits.
- Detailed error messages for troubleshooting.
