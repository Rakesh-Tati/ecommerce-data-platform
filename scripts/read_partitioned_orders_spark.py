from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ReadPartitionedOrders").getOrCreate()

df = (
    spark.read.format("parquet")
    .load("data/processed/orders_partitioned")
)

filtered_df = df.filter(col("order_date") == "2026-01-02")
filtered_df.show()
filtered_df.explain(True)


