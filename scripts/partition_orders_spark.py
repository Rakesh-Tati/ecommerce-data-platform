from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionOrders").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/processed/orders_clean_spark")
)

df.write.format("parquet").mode("overwrite").partitionBy("order_date").save(
    "data/processed/orders_partitioned"
)

print("Orders partitioned by order_date and saved to data/processed/orders_partitioned")

spark.stop()
