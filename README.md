# Data-Wrangling-ETL-Pipeline-with-Python-and-SQL
Data wrangling ETL project demonstrating extraction from multiple sources, transformation through cleaning and standardization, and loading into a structured format. Focused on building reproducible pipelines to ensure data quality and enable reliable analytics and reporting

ETL Pipeline with Python and SQL
📌 Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline built with Python for data wrangling and transformation, and SQL for querying and validation. The goal is to create a clean, consistent, and analysis-ready dataset that can be used for reporting, dashboards, or advanced analytics.

🛠 Tools & Technologies

Python (Pandas, NumPy) – Data extraction, cleaning, transformation

SQL – Querying, validation, and integration

MySQL – Database for storing and querying

🔄 Workflow Steps
1. Extract

Imported raw datasets from CSV/Excel sources (and/or APIs/databases).

Connected to SQL database for structured storage.

2. Transform

Cleaned missing values and duplicates.

Standardized column names, formats, and data types.

Applied feature engineering (e.g., derived fields, categorical encoding).

Detected and treated outliers to improve data quality.

3. Load

Loaded cleaned data into SQL database for querying.

Created normalized tables and applied constraints for consistency.

4. Query & Validate

Wrote SQL queries to:

Validate row counts and schema.

Run summary statistics for sanity checks.

Join and aggregate tables for analytics.
