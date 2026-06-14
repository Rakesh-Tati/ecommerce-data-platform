from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/raw/orders.csv")
)

print("Before Cleaning:")
df.show()

df = df.dropDuplicates()

df = df.fillna({"city": "Unknown"})

print("After Cleaning:")
df.show()