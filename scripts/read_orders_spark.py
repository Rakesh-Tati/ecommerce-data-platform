from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ecommerce Data Platform") \
    .getOrCreate()
    
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/raw/orders.csv")
    
print("First 5 Records:")
df.show(5)
    
print("Schema:")
df.printSchema()

