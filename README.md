# Health Insurance Data Warehouse & Star Schema Pipeline

## 📌 Project Overview
This project implements an end-to-end Big Data Data Warehouse solution using Apache Hadoop HDFS and Apache Spark (PySpark). 

The pipeline ingests raw health insurance data, models it into a **Star Schema** (1 Fact Table & 2 Dimension Tables), converts and persists the architecture in compressed **Parquet** format on HDFS, performs analytical SQL aggregation queries across the dimensions, and exports the reporting results back into HDFS as CSV.

---

## 🏗️ Data Warehouse Architecture (Star Schema)

### 1. Dimension Tables
- **`dim_customer`**: Holds customer demographic details (`id`, `Gender`, `Age`, `Driving_License`, `Region_Code`).
- **`dim_vehicle`**: Holds vehicle features (`vehicle_id`, `Vehicle_Age`, `Vehicle_Damage`).

### 2. Fact Table
- **`fact_policy`**: Holds insurance metrics (`customer_id`, `vehicle_id`, `Annual_Premium`, `Policy_Sales_Channel`, `Vintage`).

---

## 🛠️ Tech Stack
- **Storage**: Apache Hadoop HDFS 3.4.3
- **Processing Engine**: Apache Spark 4.1.1 / PySpark
- **Format**: Apache Parquet & CSV
- **Data Modeling**: Dimensional Star Schema (Fact & Dimensions)

---

## 🚀 Execution Steps

1. **Upload Raw Data to HDFS**:
   ```bash
   hdfs dfs -mkdir -p /data/raw
   hdfs dfs -put -f data/test.csv /data/raw/health_data.csv
spark-submit scripts/process_data.py
# Check Parquet Fact and Dimension Tables
hdfs dfs -ls /data/parquet/

# Check Analytics CSV Output
hdfs dfs -ls /data/output/star_schema_analytics_csv

