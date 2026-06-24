from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/processed/orders_clean_spark")
)

city_sales = (
    df.groupBy("city").sum("amount").withColumnRenamed("sum(amount)", "total_sales")
)
city_sales.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/city_sales_spark"
)
print("City sales data saved to 'data/analytics/city_sales'")

city_sales.show()
