from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/raw/orders.csv")
)

print("Rows:", df.count())

print("columns:", len(df.columns))

print("\ncolumns:")
print(df.columns)

print("\nNull values:")

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

print("\nDuplicate rows:")

print(df.count() - df.dropDuplicates().count())
