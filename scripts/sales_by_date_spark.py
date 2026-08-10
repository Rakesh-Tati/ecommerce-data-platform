from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("SalesByDate").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/processed/orders_clean_spark")
)

daily_sales_df = (
    df.groupBy("order_date")
    .agg(spark_sum("amount").alias("total_sales"))
    .orderBy("order_date")
)

daily_sales_df.write.format("csv").option("header", "true").save(
    "data/analytics/sales_by_date_spark"
)

print(
    "Sales by date analysis completed and saved to data/analytics/sales_by_date_spark"
)

daily_sales_df.show()

spark.stop()