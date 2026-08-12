from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SparkSQLAnalytics").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/processed/orders_clean_spark")
)
df.createOrReplaceTempView("orders")

city_sales_df = spark.sql("""
    SELECT
        city,
        SUM(amount) AS total_sales
    FROM orders
    GROUP BY city
    ORDER BY total_sales DESC
""")
city_sales_df.show()
city_sales_df.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/spark_sql/city_sales"
)

customer_rank_df = spark.sql("""
    SELECT
        customer_name,
        total_sales,
        RANK() OVER (
            ORDER BY total_sales DESC
        ) AS sales_rank
    FROM (
        SELECT
            customer_name,
            SUM(amount) AS total_sales
        FROM orders
        GROUP BY customer_name
    )
""")
customer_rank_df.show()
customer_rank_df.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/spark_sql/customer_ranking"
)


top_3_customers_df = customer_rank_df.filter(col("sales_rank") <= 3)
top_3_customers_df.show()
top_3_customers_df.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/spark_sql/top_customers"
)
daily_sales_df = spark.sql("""
    SELECT
        order_date,
        SUM(amount) AS total_sales
    FROM orders
    GROUP BY order_date
    ORDER BY order_date
""")

daily_sales_df.show()
daily_sales_df.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/spark_sql/daily_sales"
)

spark.stop()
