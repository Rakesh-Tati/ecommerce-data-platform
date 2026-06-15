from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/raw/orders.csv")
)

print("Rows before cleaning:", df.count())

df = df.dropDuplicates()

df = df.fillna({"city": "Unknown"})

print("Rows after cleaning:", df.count())

df.write.format("csv").mode("overwrite").option("header", "true").save(
    "data/processed/orders_clean_spark"
)

print("Cleaned data saved to 'data/processed/orders_clean_spark'")