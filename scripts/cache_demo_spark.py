from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/raw/orders.csv")
)

df.cache()

print("Count:", df.count())
print("\nShow:")
df.show()

print("\nGroup By:")
df.groupBy("city").count().show()

spark.stop()