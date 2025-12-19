============================================================
ShopZada Enterprise Data Warehouse
============================================================

Project Type:
Enterprise Data Warehouse (End-to-End)

Client:
ShopZada (E-commerce Platform)

Role:
Data Engineering Consultant Team

------------------------------------------------------------
1. PROJECT OVERVIEW
------------------------------------------------------------

ShopZada is a fast-growing e-commerce platform with data spread
across multiple departments and file formats. As the business
scaled, analytics became difficult due to fragmented data,
inconsistent schemas, and lack of a centralized warehouse.

This project implements a complete enterprise-grade Data
Warehouse (DWH) solution that ingests raw data, applies ETL
transformations, models data using Kimball methodology, and
delivers analytics-ready outputs for business dashboards.

The pipeline covers the full lifecycle:
- Raw data ingestion
- Staging and cleaning
- Dimensional modeling
- Analytics generation
- Dashboard consumption

------------------------------------------------------------
2. PROJECT OBJECTIVES
------------------------------------------------------------

- Integrate heterogeneous datasets from all departments
- Preserve raw data for auditability
- Apply Kimball dimensional modeling (Facts & Dimensions)
- Automate ETL using Python and Apache Airflow
- Containerize infrastructure with Docker
- Generate analytics datasets for Power BI
- Ensure reproducibility and traceability

------------------------------------------------------------
3. HIGH-LEVEL ARCHITECTURE
------------------------------------------------------------

Raw Sources
   |
   v
Staging Layer (Python ETL)
   |
   v
Dimensional Model (Facts & Dimensions)
   |
   v
Analytics Views / Aggregates
   |
   v
Power BI Dashboard

------------------------------------------------------------
4. TECHNOLOGY STACK
------------------------------------------------------------

- Python (pandas)        : Data ingestion and transformations
- Apache Airflow         : Workflow orchestration
- PostgreSQL             : Data warehouse backend
- Docker / Docker Compose: Infrastructure & environment
- Power BI               : Business intelligence dashboards

------------------------------------------------------------
5. REPOSITORY STRUCTURE
------------------------------------------------------------

.
├── sources/                 # Raw data (original formats, unchanged)
├── transformations/         # Python ETL & staging scripts
├── staging/                 # Cleaned Parquet outputs
├── models/
│   ├── dimensions/          # Dimension table SQL
│   ├── facts/               # Fact table SQL
│   └── analytics/views/     # Analytical SQL views
├── pipelines/               # Airflow DAGs
├── dashboards/
│   └── data/                # CSVs for Power BI
├── infra/                   # Docker Compose setup
├── docs/                    # Technical documentation
├── create_dashboard_csvs.py # Analytics dataset generator
└── README.txt

------------------------------------------------------------
6. DATA SOURCES
------------------------------------------------------------

Departments:
- Business            : Product catalog
- Customer Management : Users, jobs, credit cards
- Enterprise          : Merchants, staff
- Marketing           : Campaigns, transactional campaigns
- Operations          : Orders, order items, delivery delays

Supported formats:
CSV, Parquet, JSON, HTML, Excel, Pickle

NOTE:
Raw data is never modified. All transformations happen downstream.

------------------------------------------------------------
7. ETL & STAGING LAYER
------------------------------------------------------------

ETL scripts are written in Python and located in /transformations.

Responsibilities:
- Schema normalization
- Column standardization
- Data type enforcement
- Invalid record filtering
- Basic data quality checks

Output:
- Cleaned, standardized Parquet files in /staging

------------------------------------------------------------
8. DATA QUALITY CHECKS
------------------------------------------------------------

Implemented checks include:
- Empty dataset validation
- Required column existence
- Null checks on business keys
- Row count validation between stages

------------------------------------------------------------
9. DIMENSIONAL MODEL (KIMBALL)
------------------------------------------------------------

Fact Tables:
- fact_orders
  Grain: 1 row per order

- fact_order_items
  Grain: 1 row per product per order

Dimension Tables:
- dim_customer
- dim_product
- dim_campaign
- dim_merchant
- dim_date

All dimensions use SCD Type 1.

------------------------------------------------------------
10. ANALYTICS & REPORTING
------------------------------------------------------------

Analytics datasets generated as CSVs:
- Revenue by product
- Revenue by product category
- Revenue by date
- Campaign performance
- Customer lifetime value (CLV)
- KPI summary

These CSVs are consumed by Power BI dashboards.

------------------------------------------------------------
11. AIRFLOW ORCHESTRATION
------------------------------------------------------------

- DAG defined in: pipelines/dwh_pipeline_dag.py
- Each staging script runs as a task
- Explicit task dependencies
- Manual trigger (for demo and debugging)

------------------------------------------------------------
12. DOCKER INFRASTRUCTURE
------------------------------------------------------------

Services:
- PostgreSQL
- Apache Airflow (webserver + scheduler)

Run environment:
cd infra
docker compose up

Airflow UI:
http://localhost:8080
username: admin
password: admin

------------------------------------------------------------
13. HOW TO RUN (END TO END)
------------------------------------------------------------

1. Clone repository
git clone https://github.com/ChristianMichaelDelosSantos/dwh_finalproject_3CSC_group_pangkat5.git
cd dwh_finalproject_shopzada

2. Run staging scripts
python transformations/stage_orders.py
python transformations/stage_order_items.py
python transformations/stage_customers.py
python transformations/stage_products.py
python transformations/stage_merchants.py
python transformations/stage_campaigns.py

3. Generate analytics datasets
python create_dashboard_csvs.py

4. Open Power BI and load CSVs from dashboards/data

------------------------------------------------------------
14. TESTING & TRACEABILITY
------------------------------------------------------------

- Modify raw source data
- Re-run ETL pipeline
- Regenerate analytics CSVs
- Refresh Power BI dashboard

Provides full traceability from source to dashboard.

------------------------------------------------------------
15. FUTURE IMPROVEMENTS
------------------------------------------------------------

- Implement SCD Type 2 for selected dimensions
- Replace CSV analytics with live database views
- Add automated data validation reporting
- Enable scheduled Airflow runs
- Deploy to cloud infrastructure

------------------------------------------------------------
END OF FILE
============================================================