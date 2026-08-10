from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLAnalytics").getOrCreate()

df = spark.read.format("parquet").load("data/processed/orders_clean_spark")
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
spark.stop()
