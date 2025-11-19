# Azure Data Engineering Project – Incremental Data Ingestion (Task-1)

## 📌 Overview
This project demonstrates a complete **Azure Data Engineering pipeline** built using **Azure Data Factory (ADF)**, **Azure Data Lake Storage Gen2**, and **Azure SQL Database**.  
The pipeline performs **incremental data ingestion** using dynamic datasets and metadata-driven architecture.

## 📁 Process Workflows
    ┌──────────────────────────┐
    │     Source Systems       │
    │   (Azure SQL Database)   │
    └────────────┬─────────────┘
                 │
                 ▼
    ┌──────────────────────────┐
    │  Azure Data Factory      │
    │  • Incremental Pipeline  │
    │  • Dynamic Parameters    │
    │  • Mapping Data Flows    │
    └────────────┬─────────────┘
                 │
                 ▼
    ┌──────────────────────────┐
    │ Azure Data Lake Gen2     │
    │  • Raw (JSON/CSV)        │
    │  • Processed (Parquet)   │
    └──────────────────────────┘


## 🚀 Architecture
Raw → ADF → Data Lake → Parquet

## ✨ Key Features
- Incremental ingestion  
- Dynamic pipelines  
- Metadata-driven ETL  
- Parquet conversion  

## 🛠️ Technologies
ADF, ADLS, Azure SQL, Parquet  

## ▶️ How to Use
1. Import ARM template  
2. Configure linked services  
3. Deploy datasets & pipelines  
4. Run the pipeline  

# Spotify End-to-End Data Engineering Project (Databricks + Medallion Architecture) (Task-2 Transformations)

## 📌 Overview
This project implements a full Medallion Architecture (Bronze → Silver → Gold) using Databricks for Spotify analytics.

## 📁 Folder Structure
       ┌───────────────────────────────┐
       │           Bronze               │
       │ Raw ingested Spotify data      │
       └──────────────┬────────────────┘
                      │
                      ▼
       ┌───────────────────────────────┐
       │            Silver              │
       │ Cleaned, normalized data       │
       │ Conformed dimensions           │
       └──────────────┬────────────────┘
                      │
                      ▼
       ┌───────────────────────────────┐
       │             Gold               │
       │ Star Schema Models             │
       │ Fact & Dimension Tables        │
       └───────────────────────────────┘


## ✨ Features
- Modular transformations  
- Gold layer star schema  
- Delta Lake optimized pipeline  

## 🛠️ Technologies
Databricks, Delta Lake, Python, PySpark  

## ▶️ How to Run
1. Load Bronze data  
2. Run Silver scripts  
3. Run Gold transformations  
4. Query tables  

## 👤 Author
Rahul Mandaviya

