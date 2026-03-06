# CareFusion360 Data Engineering Pipeline

## Project Overview

CareFusion360 is an end-to-end healthcare data engineering pipeline designed to integrate multiple healthcare data sources and build a unified **Member360 dataset** for analytics and reporting.

The pipeline processes member, claims, policy, and event data using **PySpark-based ETL jobs** and stores the transformed datasets in a **layered data lake architecture (Bronze, Silver, Gold)**.

---

## Business Problem

Healthcare organizations receive fragmented data from multiple systems such as:

* Historical member records
* Claims and billing systems
* Policy management systems
* Real-time activity events

Without a unified data pipeline, teams face:

* Inconsistent reporting
* Delayed insights
* Data quality issues

This pipeline consolidates these sources into a **single trusted Member360 dataset** used for downstream analytics.

---

## Architecture

```
        Source Systems
   (Historical, Snowflake, APIs)
                │
                ▼
           Raw Data Layer
                │
                ▼
        PySpark ETL Processing
     (Bronze → Silver → Gold)
                │
                ▼
            Member360
        Analytics Dataset
```

---

## Technology Stack

* **PySpark**
* **AWS S3 (Data Lake Storage)**
* **Python**
* **ETL Pipeline Design**
* **Git & GitHub**
* **Data Lake Architecture**

---

## Data Pipeline Layers

### Bronze Layer

Raw ingested data from source systems.

### Silver Layer

Cleaned and standardized datasets.

### Gold Layer

Business-ready analytics datasets including **Member360**.

---

## Pipeline Jobs

| Job  | Description                       |
| ---- | --------------------------------- |
| Job1 | Historical member ingestion       |
| Job2 | Snowflake table ingestion         |
| Job3 | Web API event ingestion           |
| Job4 | Member360 gold dataset generation |

---

## Key Features

* Modular PySpark ETL jobs
* Layered data lake architecture
* Partitioned Parquet datasets
* Scalable cloud-ready pipeline design
* Git-based version control

---

## Future Improvements

* Airflow orchestration
* CI/CD deployment
* Data quality validation
* Cloud monitoring and alerting

---

## Author

Vaibhav Kamble
