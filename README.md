📦 ecommerce-weekly-kpi-data-pipeline
Overview

This project implements an automated batch data pipeline to generate weekly KPI reports for an e-commerce operations workflow.

The pipeline replaces manual Excel-based reporting by:

Ingesting weekly SKU processing data (CSV)

Applying custom business-week logic

Transforming and cleaning raw operational data

Generating KPI aggregates

Storing historical metrics using a layered Bronze–Silver–Gold architecture

Managing file lifecycle (incoming → processed)

Running on a scheduled weekly Databricks job

The objective is to simulate a production-style batch reporting system using PySpark and Databricks.

Business Context

In a typical e-commerce operations environment, teams process thousands of SKUs daily. Weekly reporting traditionally involves:

Counting processed SKUs

Calculating approval vs rejection rates

Identifying top rejection reasons

Measuring category-level rejection share

Tracking team productivity

These reports were previously generated manually using Excel pivot tables.

This project automates that workflow through a structured data pipeline.

Architecture

The system follows a layered data architecture:

Bronze Layer (Raw Data)

Stores incoming weekly CSV files.

Maintains file lifecycle:

/bronze/incoming/

/bronze/processed/

Ensures raw data immutability.

Silver Layer (Cleaned Fact Table)

Cleans and standardizes raw data.

Adds:

date

week_start_date (custom business week logic)

Stored in append mode.

Partitioned by week_start_date.

Gold Layer (KPI Tables)

Aggregated historical KPI tables:

Date-wise SKU count

Approval / Rejection percentage

Top rejection reasons

Category share within rejection reasons

Team productivity metrics

Characteristics:

Append mode (historical storage)

Partitioned by week_start_date

Includes load_timestamp

Orchestration

Databricks Job Scheduler

Weekly trigger aligned with business calendar

Fully automated batch execution

Data Flow

CSV file lands in Bronze incoming folder.

Spark batch job reads raw CSV.

Transformation logic applied:

Data type normalization

Business week calculation

Cleaned data written to Silver.

KPI aggregations generated and written to Gold.

Processed file moved to Bronze processed folder.

Job completes.

Key Architectural Decisions

Batch processing chosen due to weekly reporting cadence.

Bronze–Silver–Gold separation for clear data layering.

Partitioning by week_start_date for efficient historical queries.

Append mode for KPI tables to preserve reporting history.

Window functions used for percentage and share calculations.

File lifecycle management to prevent duplicate ingestion.

Load timestamps stored for traceability.

Tech Stack

PySpark

Databricks

Parquet storage

Window functions

Databricks Job Scheduler

Partitioned batch processing

Example KPIs Generated

Total SKUs processed per day

Weekly approval vs rejection percentage

Top rejection reasons (with percentage contribution)

Category-wise share within rejection reasons

Team member productivity metrics

Future Improvements

Potential enhancements for production-grade systems:

Incremental reprocessing safeguards

Schema evolution handling

Data quality validation layer

Centralized logging and alerting

Delta Lake implementation with merge/upsert

CI/CD deployment automation

Repository Structure
ecommerce-weekly-kpi-data-pipeline/
│
├── README.md
├── architecture/
│     └── pipeline-architecture.png
├── notebooks/
│     └── ecommerce_weekly_pipeline.py
├── sample_data/
│     └── sample_sku_week.csv
Conclusion

This project demonstrates the design and implementation of a structured, automated, batch-oriented KPI reporting pipeline with clear data layering, historical storage strategy, and orchestration.

It focuses on architectural clarity, business metric correctness, and scalable data engineering practices.
