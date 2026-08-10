from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/processed/orders_clean_spark")
)
     
city_sales = (
    df.groupBy("city")
    .agg(spark_sum("amount").alias("total_sales"))
    .orderBy("total_sales", ascending=False))

city_sales.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/city_sales_spark"
)

print("City sales data saved to 'data/analytics/city_sales'")

city_sales.show()


