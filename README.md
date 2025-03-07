### bakehouse-assessment

This repository provides a solution for a Databricks Data Engineering Assessment using PySpark and Delta Lake. It is organized into a layered architecture and includes workflows with Delta Live Tables.
The project implements two pipelines in Databricks to fulfill the assessment requirements, with a focus on best practices and proper orchestration.

## Pipeline Silver

# Objective:
Transform and clean raw data from Bakehouse and store the results in the schema jp_assessment.latam_lab_silver.

# Automation:
Executed via a Job configured with cron (Monday to Friday at 8:00 AM) that uses a job cluster, email notifications, and a 20-minute timeout.

# Implementation:
Logic was developed in notebooks (02_Staging.ipynb and 03_Silver.ipynb) to facilitate interactive exploration and debugging.

# Additional Notes:
An EDA stage was implemented to understand the context and table structure before starting the ETL process.
A Staging layer (not required by the assessment) was added as a best practice to validate and transform data before loading it into Silver.

## Pipeline Gold

# Objective:
Generate two analytical tables from Silver data:

# transactions_per_product: 
A pivot table of transactions, where product names are sanitized (spaces replaced with underscores) to avoid errors due to invalid characters.

# transactions_details: 
A join of transactions with customer data to enrich the information.

# Automation:
A DLT pipeline is configured to start 5 minutes after the Silver pipeline, ensuring that data is updated.

# Implementation:
The Gold pipeline logic is implemented in a Python file (dlt_pipelines/gold_pipeline.py) rather than in a notebook. This approach allows for better modularity and version control.

## Challenges Faced

# NO_TABLES_IN_PIPELINE Error:
The pipeline initially did not detect functions decorated with @dlt.table. This was resolved using the UI.

# Invalid Column Names:
A conflict with column names (e.g., "Austin Almond Biscotti") was resolved by sanitizing names (replacing spaces with underscores).

## Scheduling Alternatives Considered

# Independent Pipelines with Their Own Cron:

Configuration: Silver at 8:00 AM and Gold at, for example, 8:05 AM.
Advantage: Simple to configure.
Disadvantage: Relies solely on time difference, with no guarantee that Silver finishes before Gold starts.

# Single Job with Dependent Tasks:

Configuration: Integrate Staging, Silver, and Gold in one job, with Gold set to depend on Silver.
Advantage: Guarantees Gold only runs if Silver completes successfully.
Disadvantage: Deviates from the assignment’s requirement for two independent pipelines.

# Invoking the DLT API at the End of the Silver Pipeline:

Configuration: Add a final task in the Silver job that triggers Gold via the API.
Advantage: Maintains separate pipelines with explicit dependency.
Disadvantage: Increases complexity with an extra script.

# Decision Taken:
I chose to configure the Gold pipeline with a staggered schedule—programming it to start 5 minutes after Silver. This respects the requirement for two independent pipelines while reducing the risk of Gold starting before Silver finishes, without combining them into a single job or using additional scripts.

# Project Structure

bakehouse-assessment/
├── README.md                          # This document
├── notebooks/
│   ├── 01_EDA.ipynb                   # Exploratory Analysis
│   ├── 02_Staging.ipynb               # Initial Transformation (Staging)
│   └── 03_Silver.ipynb                # Silver Pipeline
├── dlt_pipelines/
│   └── gold_pipeline.py               # DLT code for the Gold Pipeline
└── Jobs/
    ├── silver_pipeline_job.json       # Silver Job configuration
    └── gold_pipeline_dlt.json         # Gold DLT Pipeline configuration


