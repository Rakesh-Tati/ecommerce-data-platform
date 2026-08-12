from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


dag = DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 8, 11),
    schedule=None,
    catchup=False,
    max_active_runs=1,

    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },

    dagrun_timeout=timedelta(minutes=30),
)


profile_orders = BashOperator(
    task_id="profile_orders",
    bash_command="python scripts/profile_orders_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


validate_orders = BashOperator(
    task_id="validate_orders",
    bash_command="python scripts/validate_orders_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


clean_orders = BashOperator(
    task_id="clean_orders",
    bash_command="python scripts/clean_orders_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


customer_sales = BashOperator(
    task_id="customer_sales",
    bash_command="python scripts/customer_sales_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


city_sales = BashOperator(
    task_id="city_sales",
    bash_command="python scripts/city_sales_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


total_revenue = BashOperator(
    task_id="total_revenue",
    bash_command="python scripts/total_revenue_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


sales_by_date = BashOperator(
    task_id="sales_by_date",
    bash_command="python scripts/sales_by_date_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


partition_orders = BashOperator(
    task_id="partition_orders",
    bash_command="python scripts/partition_orders_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


join_customers_orders = BashOperator(
    task_id="join_customers_orders",
    bash_command="python scripts/join_customers_orders_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


spark_sql_analytics = BashOperator(
    task_id="spark_sql_analytics",
    bash_command="python scripts/spark_sql_analytics.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


incremental_load = BashOperator(
    task_id="incremental_load",
    bash_command="python scripts/incremental_load_spark.py",
    cwd="/mnt/d/ecommerce-data-platform",
    dag=dag,
)


profile_orders >> validate_orders >> clean_orders

clean_orders >> customer_sales
customer_sales >> city_sales
city_sales >> total_revenue

total_revenue >> sales_by_date
sales_by_date >> partition_orders
partition_orders >> join_customers_orders
join_customers_orders >> spark_sql_analytics
spark_sql_analytics >> incremental_load