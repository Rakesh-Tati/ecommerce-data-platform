from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, sum as spark_sum, when


spark = (
    SparkSession.builder
    .appName("Ecommerce Validate Orders")
    .getOrCreate()
)

try:
    # ---------------------------------------------------------
    # 1. Read source data
    # ---------------------------------------------------------
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/raw/orders.csv")
    )

    # ---------------------------------------------------------
    # 2. Required columns
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
            f"Data quality check failed: missing columns: "
            f"{sorted(missing_columns)}"
        )

    # ---------------------------------------------------------
    # 3. Total records
    # ---------------------------------------------------------
    total_records = df.count()
    print(f"Total records: {total_records}")

    if total_records == 0:
        raise ValueError(
            "Data quality check failed: source contains no records."
        )

    # ---------------------------------------------------------
    # 4. NULL checks
    # ---------------------------------------------------------
    null_values = df.select(
        [
            spark_sum(
                when(col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in df.columns
        ]
    )

    print("Null values:")
    null_values.show()

    critical_columns = [
        "order_id",
        "customer_id",
        "customer_name",
        "amount",
        "order_date",
    ]

    critical_null_condition = reduce(
        lambda a, b: a | b,
        [col(c).isNull() for c in critical_columns],
    )

    critical_nulls = df.filter(
        critical_null_condition
    ).count()

    if critical_nulls > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{critical_nulls} records contain NULL values "
            f"in critical columns."
        )

    # ---------------------------------------------------------
    # 5. Duplicate check
    # ---------------------------------------------------------
    duplicate_count = (
        df.count() - df.dropDuplicates().count()
    )

    print(f"Duplicate Records: {duplicate_count}")

    if duplicate_count > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{duplicate_count} duplicate records found."
        )

    # ---------------------------------------------------------
    # 6. Negative amount check
    # ---------------------------------------------------------
    negative_amount_count = (
        df.filter(col("amount") < 0).count()
    )

    print(
        f"Records with negative amounts: "
        f"{negative_amount_count}"
    )

    if negative_amount_count > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{negative_amount_count} records have negative amounts."
        )

    # ---------------------------------------------------------
    # 7. Future date check
    # ---------------------------------------------------------
    future_date_count = (
        df.filter(col("order_date") > current_date()).count()
    )

    print(
        f"Future Date Records: "
        f"{future_date_count}"
    )

    if future_date_count > 0:
        raise ValueError(
            f"Data quality check failed: "
            f"{future_date_count} records have future order dates."
        )

    # ---------------------------------------------------------
    # 8. Validation successful
    # ---------------------------------------------------------
    print("========================================")
    print("DATA QUALITY VALIDATION PASSED")
    print("========================================")

finally:
    spark.stop()





