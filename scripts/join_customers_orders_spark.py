from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, rank, col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()


def read_csv(path):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )

orders_df = read_csv("data/raw/orders.csv")
customers_df = read_csv("data/raw/customers.csv").withColumnRenamed(
    "customer_name", "customer_name_customer"
)

joined_df = orders_df.join(customers_df, on="customer_id", how="left")

customers_sales_df = (
    joined_df.groupBy("customer_name_customer")
    .agg(spark_sum("amount").alias("total_sales"))
    .orderBy("total_sales", ascending=False)
)

window_spec = Window.orderBy(col("total_sales").desc())
customers_sales_df = customers_sales_df.withColumn(
    "sales_rank", rank().over(window_spec)
)

top_customers_df = customers_sales_df.filter(col("sales_rank") <= 3)

top_customers_df.show()

spark.stop()
