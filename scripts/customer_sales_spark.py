from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/processed/orders_clean_spark")
)

customer_sales = (
    df.groupBy("customer_name")
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_sales")
    .orderBy("total_sales", ascending=False)
)

customer_sales.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/analytics/customer_sales_spark"
)

print("Customer sales data saved to 'data/analytics/customer_sales_spark'")
customer_sales.show()
