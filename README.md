
# Real-Time Hospital Patient Flow & Bed Occupancy Analytics

### An End-to-End Real-Time Data Engineering Project Using Azure, Databricks, PySpark and Power BI

[![Azure](https://img.shields.io/badge/Microsoft_Azure-0089D6?style=flat&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Databricks](https://img.shields.io/badge/Databricks-EF3E2F?style=flat&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-FDEE21?style=flat&logo=apachespark&logoColor=black)](https://spark.apache.org/docs/latest/api/python/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=flat&logo=delta&logoColor=white)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

---

## Table of Contents

1. [Project Overview](#-project-overview)
2. [Business Problem](#-business-problem)
3. [Project Objectives](#-project-objectives)
4. [Solution Architecture](#-solution-architecture)
5. [Technology Stack](#-technology-stack)
6. [Data Engineering Workflow](#-data-engineering-workflow)
   - [Data Ingestion](#1-data-ingestion--azure-event-hubs)
   - [Bronze Layer](#2-bronze-layer--raw-data)
   - [Silver Layer](#3-silver-layer--cleaned-data)
   - [Gold Layer](#4-gold-layer--business-ready-data)
7. [Dimensional Modelling](#-dimensional-modelling--star-schema)
8. [Slowly Changing Dimension Type 2](#-slowly-changing-dimension-type-2-scd-type-2)
9. [Pipeline Orchestration](#-pipeline-orchestration--azure-data-factory)
10. [Data Governance and Security](#-data-governance--security)
11. [Databricks SQL and Power BI](#-databricks-sql--power-bi-analytics)
12. [Business Intelligence KPIs](#-business-intelligence-kpis)
13. [Repository Structure](#-repository-structure)
14. [Key Engineering Decisions](#-key-engineering-decisions)
15. [Future Improvements](#-future-improvements)
16. [Project Learnings](#-project-learnings)

---

## 📌 Project Overview

This project demonstrates the development of an end-to-end real-time healthcare data platform designed to monitor patient admissions, discharges, department workloads and hospital bed occupancy.

The solution simulates patient admission and discharge events and processes them through a streaming data pipeline built with Azure Event Hubs, Apache Kafka and Azure Databricks.

Using PySpark and Delta Lake, incoming data is processed through a Medallion Architecture consisting of Bronze, Silver and Gold layers.

The Gold layer implements dimensional modelling, including a Star Schema and Slowly Changing Dimension Type 2 (SCD Type 2), to support historical tracking and analytical reporting.

Azure Data Factory orchestrates the processing workflow, while Unity Catalog provides centralized data governance. Databricks SQL serves as the analytical warehouse, with Power BI providing the business intelligence layer.

**The objective is to demonstrate how streaming data can be transformed into governed, analytics-ready information for operational decision-making.**

## 🏥 Business Problem

Great Lakes Regional Medical Center (GLRMC) represents a network of seven hospitals across the Midwest region.

The organization faces challenges in monitoring patient flow, particularly during periods of high demand, such as seasonal flu outbreaks.

Without a centralized data platform, hospital management has limited visibility into:

- Current bed occupancy across hospital departments.
- Patient admission and discharge patterns.
- Department-level workloads and bottlenecks.
- Patient demographics and occupancy distribution.
- Average patient length of stay.

The proposed solution addresses these challenges by creating a centralized data platform that processes patient activity and makes operational information available for analytics.

## 🎯 Project Objectives

The project focuses on:

- Implementing real-time data ingestion using Azure Event Hubs and Kafka.
- Building a scalable Medallion Architecture using Azure Databricks.
- Processing and transforming streaming data with PySpark.
- Handling schema evolution and data quality issues.
- Implementing SCD Type 2 for historical patient demographic tracking.
- Designing a Star Schema for analytical workloads.
- Automating pipeline execution using Azure Data Factory.
- Implementing centralized governance through Unity Catalog.
- Securing connection strings and credentials using Azure Key Vault.
- Delivering business intelligence dashboards using Power BI.

---

## 🏗️ Solution Architecture

The architecture follows an end-to-end data engineering workflow:

**Data Source → Azure Event Hubs → Databricks Medallion Architecture → Databricks SQL → Power BI**

### Architecture Diagram

<img width="1918" height="820" alt="Azure Data Platform Architecture Flow" src="https://github.com/user-attachments/assets/e0e92200-1126-4f81-aecb-1b0a35cc3b41" />



*End-to-end architecture showing streaming ingestion, Medallion data processing, Unity Catalog governance, Databricks SQL and Power BI.*

### Architecture Components

| Component | Responsibility |
|---|---|
| Python Kafka Simulator | Generates simulated patient admission and discharge events |
| Azure Event Hubs | Provides the streaming ingestion platform |
| Apache Kafka | Provides the Kafka-compatible streaming interface |
| Azure Data Lake Storage Gen2 | Stores the Bronze, Silver and Gold data |
| Azure Databricks | Performs distributed data processing |
| PySpark | Handles data transformation and cleansing |
| Delta Lake | Provides reliable data storage |
| Unity Catalog | Centralized data governance and access control |
| Azure Data Factory | Orchestrates the data processing workflow |
| Azure Key Vault | Securely stores connection strings, tokens and credentials |
| Databricks SQL | Serves business-ready analytical data |
| Power BI | Provides operational dashboards and visualizations |

---

## 🛠️ Technology Stack

### Cloud Infrastructure
- Microsoft Azure
- Azure Event Hubs
- Azure Data Lake Storage Gen2
- Azure Data Factory
- Azure Key Vault
- Microsoft Entra ID (Azure AD)

### Data Engineering
- Azure Databricks
- Apache Spark
- PySpark
- Apache Kafka
- Spark Structured Streaming
- Delta Lake
- Medallion Architecture

### Data Modelling and Governance
- Unity Catalog
- Star Schema
- Slowly Changing Dimension Type 2 (SCD Type 2)
- Surrogate Keys
- Schema Evolution
- Data Quality and Validation

### Analytics and Reporting
- Databricks SQL
- SQL Warehouse
- Power BI Desktop
- Power BI Dashboards

### Development and Version Control
- Python
- Git
- GitHub
- Databricks Notebooks

---

## ⚙️ Data Engineering Workflow

### 1. Data Ingestion – Azure Event Hubs

The pipeline starts with a Python-based Kafka simulator that generates simulated patient admission and discharge events.

The simulator publishes events to Azure Event Hubs using its Kafka-compatible interface.

Each event contains patient and hospital information.

**Incoming event schema:**

| Column | Description |
|---|---|
| patient_id | Unique patient identifier |
| gender | Patient gender |
| age | Patient age |
| department | Hospital department |
| admission_time | Patient admission timestamp |
| discharge_time | Patient discharge timestamp |
| bed_id | Hospital bed identifier |
| hospital_id | Hospital identifier |

Azure Event Hubs acts as the streaming ingestion layer, allowing Databricks to consume incoming events continuously.

### 2. Bronze Layer – Raw Data

The Bronze layer stores incoming streaming data in its original form.

Using Spark Structured Streaming, Databricks connects to Azure Event Hubs through Kafka-compatible connection options.

The incoming Kafka messages are initially received as binary values.

The processing workflow:

1. Configures the Kafka connection to Azure Event Hubs.
2. Reads the incoming streaming messages.
3. Converts binary values into JSON strings.
4. Writes the raw JSON records into the Bronze storage directory.
5. Uses Append mode to continuously add incoming records.
6. Maintains streaming checkpoints to support recovery and processing continuity.

**Key implementation concepts:**

- Spark Structured Streaming
- Kafka readStream
- Binary-to-string conversion
- Delta storage
- Append output mode
- Streaming checkpoints

### 3. Silver Layer – Cleaned Data

The Silver layer transforms raw JSON records into structured, validated data.

Data is read from the Bronze layer and processed using PySpark.

**Transformation workflow:**

1. Reads raw records from Bronze.
2. Defines an explicit PySpark schema.
3. Parses JSON strings using `from_json()`.
4. Expands the parsed structure into individual columns.
5. Converts admission and discharge timestamps using `to_timestamp()`.
6. Identifies and handles invalid records.
7. Applies schema evolution to accommodate newly introduced columns.
8. Writes the cleaned data into the Silver layer.

**Data quality scenarios addressed:**

| Data Quality Issue | Handling Strategy |
|---|---|
| Future admission timestamps | Replaces future timestamps with the current timestamp |
| Patient age greater than 100 | Replaces invalid ages with generated values between 1 and 91 |
| Missing expected columns | Adds missing columns with NULL values |
| Schema changes | Uses `mergeSchema` during writes |

Schema evolution helps accommodate changes in incoming data without requiring the pipeline to depend on an entirely fixed schema.

### 4. Gold Layer – Business-Ready Data

The Gold layer transforms cleaned Silver data into structured analytical datasets.

Unlike the streaming Bronze ingestion process, the Gold processing workflow reads the Silver dataset to compare incoming records with existing dimensional records.

This allows the implementation to identify historical changes and maintain dimension history.

The Gold layer contains three primary datasets:

- Patient Dimension
- Department Dimension
- Patient Flow Fact Table

The resulting datasets follow a Star Schema design.

---

## 📐 Dimensional Modelling – Star Schema

The Gold layer uses dimensional modelling to separate descriptive attributes from transactional patient activity.

### Data Model

**Dimension Tables**

`dim_patient`

Stores patient demographic information and historical changes.

| Column | Description |
|---|---|
| surrogate_key | Unique dimension record identifier |
| patient_id | Business patient identifier |
| gender | Patient gender |
| age | Patient age |
| effective_from | Date the record becomes effective |
| effective_to | Date the record expires |
| is_current | Indicates the current dimension record |

`dim_department`

Stores hospital and department information.

| Column | Description |
|---|---|
| surrogate_key | Unique department dimension identifier |
| department | Hospital department |
| hospital_id | Hospital identifier |

**Fact Table**

`fact_patient_flow`

Stores patient admission and discharge transactions.

| Column | Description |
|---|---|
| fact_id | Fact record identifier |
| patient_surrogate_key | Patient dimension reference |
| department_surrogate_key | Department dimension reference |
| admission_time | Patient admission timestamp |
| discharge_time | Patient discharge timestamp |
| admission_date | Admission date |
| length_of_stay_hours | Calculated patient stay duration |
| is_currently_admitted | Indicates current admission status |
| bed_id | Hospital bed identifier |
| event_ingestion_time | Fact ingestion timestamp |

### Star Schema Relationship

The fact table references the patient and department dimensions through their surrogate keys.

This structure supports analytical queries without repeatedly storing descriptive patient and department attributes in every transaction.

---

## 🔄 Slowly Changing Dimension Type 2 (SCD Type 2)

One of the important engineering components of this project is maintaining historical patient demographic information.

Patient attributes such as age and gender may change over time. Overwriting existing records would remove historical information.

To address this, I implemented a hash-based SCD Type 2 approach for the patient dimension.

### Implementation Workflow

1. Reads incoming patient records from Silver.
2. Identifies the latest record for each patient.
3. Creates a hash from demographic attributes such as gender and age.
4. Reads existing patient dimension records from Gold.
5. Generates a corresponding hash for existing records.
6. Compares incoming and existing hashes to identify demographic changes.
7. Identifies the surrogate keys of affected current records.
8. Expires the previous records by setting:
   - `is_current = false`
   - `effective_to = current_timestamp()`
9. Inserts new dimension records with new surrogate keys.
10. Marks the newly inserted records as current.

### Example

| Patient ID | Gender | Age | Is Current |
|---|---|---|---|
| P1001 | Male | 35 | FALSE |
| P1001 | Male | 36 | TRUE |

Instead of replacing the previous demographic record, the pipeline preserves the historical version and inserts a new record.

This enables historical analysis based on the demographic attributes that were valid at a particular point in time.

---

## 🔁 Pipeline Orchestration – Azure Data Factory

Azure Data Factory orchestrates the Databricks processing workflow.

The pipeline uses Databricks notebook activities to execute the Bronze, Silver and Gold processing stages.

### Pipeline Execution Flow

**Bronze Notebook → Silver Notebook → Conditional Execution → Gold Notebook**

The orchestration workflow includes:

1. Bronze notebook activity.
2. Silver notebook activity with a successful Bronze dependency.
3. An If Condition activity to determine whether Silver processed new records.
4. Gold notebook execution only when new records are available.

### Conditional Execution

The If Condition uses the following expression:

```text
@greater(int(activity('Silver').output.runOutput), 0)
```

The expression evaluates the number of newly processed records returned by the Silver notebook.

- **TRUE:** Executes the Gold notebook.
- **FALSE:** Skips Gold processing.

This prevents unnecessary downstream processing when no new records have been processed.

### Linked Services

Three linked services were configured:

- Azure Key Vault
- Azure Databricks
- Azure Data Lake Storage Gen2

The Databricks notebooks are executed through Azure Data Factory rather than directly manipulating the underlying storage paths.

This design helps preserve Unity Catalog governance when orchestration happens outside Databricks.

Pipeline failure notifications were also configured through Azure Monitor.

---

## 🔐 Data Governance & Security

Security and governance are important parts of the architecture.

### Unity Catalog

Unity Catalog provides centralized governance for the Medallion Architecture.

The implementation includes:

- A Unity Catalog metastore.
- A central catalog.
- Bronze, Silver and Gold schemas.
- External locations configured using an Azure Databricks Access Connector.
- Managed access permissions for data objects.

Unity Catalog provides a centralized approach to controlling access to data across the different processing layers.

### Azure Key Vault

Azure Key Vault is used to securely store sensitive credentials, including:

- Azure Event Hubs connection strings.
- Databricks access tokens.
- Other connection credentials.

A Databricks secret scope provides access to the required Key Vault secrets.

Access permissions are configured so that Databricks and Azure Data Factory can retrieve the secrets they require.

This avoids hardcoding sensitive credentials directly into notebooks or application code.

---

## 📊 Databricks SQL & Power BI Analytics

The Gold layer provides the business-ready datasets used for reporting.

Initially, Azure Synapse Analytics was considered as the data warehouse.

However, accessing the underlying storage directly through Synapse would bypass the Unity Catalog table-level governance established within Databricks.

To maintain centralized governance, I selected **Databricks SQL Warehouse** as the analytical serving layer.

The Gold tables are exposed through Databricks SQL and queried using SQL.

Power BI Desktop connects to Databricks SQL Warehouse through Partner Connect.

The required catalog, schema and table permissions are granted to the connecting user.

This provides a governed connection between the data engineering platform and the business intelligence layer.

---

## 📈 Business Intelligence KPIs

The Power BI dashboard focuses on operational patient flow and bed occupancy metrics.

### Key Performance Indicators

| KPI | Business Purpose |
|---|---|
| Current Bed Occupancy % | Monitors occupied beds against available capacity |
| Gender-Based Bed Occupancy | Provides demographic occupancy distribution |
| Average Length of Stay | Measures the average duration of patient admissions |
| Patient Admission Activity | Tracks incoming patient activity |
| Patient Discharge Activity | Tracks patient discharge patterns |
| Department-Level Occupancy | Helps identify departments experiencing higher demand |

The dashboard demonstrates how the Gold layer can support operational reporting and hospital resource monitoring.

---

## 📁 Repository Structure

The repository contains the project implementation, including the Python streaming simulator and Databricks notebooks.

Refer to the repository folders for the actual notebook and supporting file organization.

---

## 🧠 Key Engineering Decisions

Some of the important engineering decisions made during this project include:

**1. Using Azure Event Hubs with Kafka**

Leveraged the Kafka-compatible interface of Azure Event Hubs to ingest simulated patient events into Databricks.

**2. Using Delta Lake and Medallion Architecture**

Separated raw, cleaned and business-ready data to improve processing organization and downstream analytics.

**3. Implementing Hash-Based SCD Type 2**

Used hash comparisons to identify changes in patient demographics while preserving historical records.

**4. Conditional Orchestration with Azure Data Factory**

Executed Gold processing only when Silver successfully processed new records.

**5. Using Databricks SQL Instead of Synapse**

Maintained Unity Catalog governance by using Databricks SQL as the analytical serving layer.

**6. Centralizing Secrets with Azure Key Vault**

Avoided exposing sensitive connection credentials in notebooks and source code.

---

## 🚀 Future Improvements

Potential areas for extending the platform include:

- Integrating actual batch ingestion from Electronic Health Record (EHR) systems.
- Implementing automated data quality expectations and monitoring.
- Extending historical tracking to department dimensions.
- Introducing more advanced streaming latency and pipeline monitoring.
- Implementing automated infrastructure deployment.
- Expanding analytical dashboards with additional hospital operational KPIs.

---

## 📚 Project Learnings

Building this project provided practical experience across the data engineering lifecycle.

It reinforced the importance of designing data platforms that go beyond ingestion and transformation.

A production-oriented data engineering solution must also consider:

- Data quality and schema evolution.
- Historical data management.
- Dimensional modelling.
- Pipeline orchestration.
- Security and governance.
- Analytical serving and reporting.

The project demonstrates how Azure and Databricks services can work together to build a streaming data platform that transforms incoming events into structured, governed and analytics-ready information.

---

## 👨‍💻 Author

**Daniel Mate**

Data Engineering | Azure | Databricks | PySpark | Data Lakehouse

GitHub: [Dan008Data](https://github.com/Dan008Data)

LinkedIn:[ [Connect with me](https://www.linkedin.com/)](https://www.linkedin.com/in/daniel-mate-a260a54b/)

---

## 🏷️ GitHub Topics

`azure` `azure-databricks` `databricks` `pyspark` `apache-spark` `apache-kafka` `azure-event-hubs` `azure-data-factory` `azure-data-lake-storage` `delta-lake` `unity-catalog` `medallion-architecture` `structured-streaming` `data-engineering` `real-time-data-processing` `scd-type-2` `star-schema` `databricks-sql` `power-bi` `data-warehouse`
