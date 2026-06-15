from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max

spark = SparkSession.builder.appName("Ecommerce Incremental Load").getOrCreate()

# Read watermark
with open("metadata/watermark.txt", "r") as f:
    watermark = f.read().strip()

print(f"Current watermark: {watermark}")

# Read orders data
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/raw/orders.csv")
)

# Filter records newer than watermark
new_orders = df.filter(df.order_date > watermark)

print(f"\nNew records since {watermark}:")
new_orders.show()

processed_orders = new_orders.dropDuplicates().fillna({"city": "Unknown"})

new_record_count = processed_orders.count()

# Get the latest order_date
# if not processed_orders.rdd.isEmpty():

if new_record_count > 0:
        
    processed_orders.write.format("csv").mode("append").option("header", "true").save(
        "data/processed/incremental_orders_spark"
    )

    latest_date = processed_orders.agg(spark_max("order_date").alias("max_date")).first()[
        "max_date"
    ]

    # Update watermark
    with open("metadata/watermark.txt", "w") as f:
        f.write(str(latest_date))

    print(f"\nWatermark updated to: {latest_date}")
    print(f"Total new records processed: {new_record_count}")
else:
    print("\nNo new records to process")

spark.stop()
