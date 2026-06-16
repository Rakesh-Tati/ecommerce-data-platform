from json import load

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/raw/orders.csv")
)

cleaned_df = df.dropDuplicates().fillna({"city": "Unknown"})

cleaned_df.write.format("parquet").mode("overwrite").save(
    "data/processed/cleaned_orders_parquet")

spark.stop()

