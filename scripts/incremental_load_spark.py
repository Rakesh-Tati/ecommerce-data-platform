from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max


BASE_DIR = Path("/mnt/d/ecommerce-data-platform")
SOURCE_PATH = str(BASE_DIR / "data/raw/orders.csv")
OUTPUT_PATH = str(BASE_DIR / "data/processed/incremental_orders_spark")
WATERMARK_PATH = BASE_DIR / "metadata/watermark.txt"


spark = (
    SparkSession.builder
    .appName("Ecommerce Incremental Load")
    .getOrCreate()
)

try:
    # ---------------------------------------------------------
    # 1. Read watermark
    # ---------------------------------------------------------
    watermark = WATERMARK_PATH.read_text().strip()

    print(f"Current watermark: {watermark}")

    # ---------------------------------------------------------
    # 2. Read source data
    # ---------------------------------------------------------
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(SOURCE_PATH)
    )

    # ---------------------------------------------------------
    # 3. Basic validation
    # ---------------------------------------------------------
    required_columns = {
        "order_id",
        "customer_id",
        "customer_name",
        "city",
        "amount",
        "order_date",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    # ---------------------------------------------------------
    # 4. Find records newer than watermark
    # ---------------------------------------------------------
    new_orders = df.filter(df.order_date > watermark)

    # ---------------------------------------------------------
    # 5. Deduplicate using business key
    # ---------------------------------------------------------
    processed_orders = (
        new_orders
        .dropDuplicates(["order_id"])
        .fillna({"city": "Unknown"})
    )

    new_record_count = processed_orders.count()

    print(f"New records found: {new_record_count}")

    # ---------------------------------------------------------
    # 6. Process only when new records exist
    # ---------------------------------------------------------
    if new_record_count > 0:

        print("New records to process:")
        processed_orders.show(truncate=False)

        # Write new records
        (
            processed_orders
            .write
            .format("csv")
            .mode("append")
            .option("header", "true")
            .save(OUTPUT_PATH)
        )

        # -----------------------------------------------------
        # 7. Get latest processed date
        # -----------------------------------------------------
        latest_date = (
            processed_orders
            .agg(
                spark_max("order_date").alias("max_date")
            )
            .first()["max_date"]
        )

        # -----------------------------------------------------
        # 8. Update watermark ONLY after successful write
        # -----------------------------------------------------
        WATERMARK_PATH.write_text(str(latest_date))

        print(f"Watermark updated to: {latest_date}")
        print(f"Total new records processed: {new_record_count}")

    else:
        print("No new records to process.")

finally:
    spark.stop()