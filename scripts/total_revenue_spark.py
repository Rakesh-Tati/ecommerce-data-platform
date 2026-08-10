from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/processed/orders_clean_spark")
)

total_revenue = df.agg(spark_sum("amount").alias("total_revenue"))

total_revenue.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/total_revenue_spark"
)
print("Total revenue data saved to 'data/analytics/total_revenue_spark'")
total_revenue.show()
spark.stop()
