import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when


# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("profile_orders")


# ---------------------------------------------------------
# Spark session
# ---------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Ecommerce Data Platform - Profile Orders")
    .getOrCreate()
)


try:
    logger.info("Starting orders profiling")

    # -----------------------------------------------------
    # 1. Read source data
    # -----------------------------------------------------
    logger.info("Reading source data: data/raw/orders.csv")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/raw/orders.csv")
    )

    # -----------------------------------------------------
    # 2. Basic profile
    # -----------------------------------------------------
    row_count = df.count()
    column_count = len(df.columns)

    logger.info("Rows: %s", row_count)
    logger.info("Columns: %s", column_count)
    logger.info("Column names: %s", df.columns)

    # -----------------------------------------------------
    # 3. NULL profile
    # -----------------------------------------------------
    logger.info("Calculating NULL values")

    null_values = df.select(
        [
            count(
                when(col(c).isNull(), c)
            ).alias(c)
            for c in df.columns
        ]
    )

    logger.info("NULL value profile:")
    null_values.show()

    # -----------------------------------------------------
    # 4. Duplicate profile
    # -----------------------------------------------------
    logger.info("Calculating duplicate rows")

    duplicate_count = (
        df.count()
        - df.dropDuplicates().count()
    )

    logger.info("Duplicate rows: %s", duplicate_count)

    # -----------------------------------------------------
    # 5. Profiling completed
    # -----------------------------------------------------
    logger.info("Orders profiling completed successfully")

finally:
    logger.info("Stopping Spark session")
    spark.stop()
