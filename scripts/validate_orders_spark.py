from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum
from pyspark.sql.functions import current_date

spark = SparkSession.builder.appName("Ecommerce Validate Orders").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/raw/orders.csv")
)

# Total Records
total_records = df.count()
print(f"Total records: {total_records}")

# 2. Null Values
null_values = df.select(
    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
)
print("Null values in critical fields:")
null_values.show()

# Duplicate Records
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"Duplicate Records: {duplicate_count}")

negative_amount_count = df.filter(col("amount") < 0).count()
print(f"Records with negative amounts: {negative_amount_count}")

future_date_count = df.filter(col("order_date") > current_date()).count()
print(f"Future Date Records: {future_date_count}")

spark.stop()
