# E-Commerce Data Platform

An end-to-end Data Engineering project built using Python, PySpark, Apache Airflow and Git.

## Architecture

Raw CSV
→ Profiling
→ Validation
→ Cleaning
→ Analytics
→ Partitioning
→ Join
→ Spark SQL
→ Incremental Load
→ Airflow Orchestration

## Technologies

- Python
- PySpark
- Apache Airflow
- Spark SQL
- Pandas
- CSV / Parquet
- Git / GitHub

## Pipeline

The pipeline processes e-commerce order data and produces:

- Customer-wise sales
- City-wise sales
- Total revenue
- Sales by date
- Partitioned order data
- Joined customer/order data
- Spark SQL analytics
- Incremental order processing using a watermark

## Incremental Processing

The pipeline maintains the latest processed `order_date` in:

`metadata/watermark.txt`

Only records newer than the watermark are processed.

## Airflow DAG

DAG:

`ecommerce_pipeline`

Main task flow:

profile_orders
→ validate_orders
→ clean_orders
→ customer_sales
→ city_sales
→ total_revenue
→ sales_by_date
→ partition_orders
→ join_customers_orders
→ spark_sql_analytics
→ incremental_load

## Running the Project

Activate the virtual environment and execute the required PySpark scripts.

Airflow is used to orchestrate the complete workflow.

## Project Outcome

The project demonstrates core Data Engineering concepts including:

- ETL
- Data quality
- PySpark transformations
- Aggregations
- Joins
- Partitioning
- Spark SQL
- Incremental processing
- Workflow orchestration
- Version control
