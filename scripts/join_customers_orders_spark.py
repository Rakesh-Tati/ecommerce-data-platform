from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("Ecommerce Data Platform").getOrCreate()


def read_csv(path):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )


orders_df = read_csv("data/raw/orders.csv")
customers_df = read_csv("data/raw/customers.csv")

joined_df = orders_df.join(customers_df, on="customer_id", how="left")

customers_sales_df = (
    joined_df.groupBy("customer_name")
    .agg(sum("amount").alias("total_sales"))
    .orderBy("total_sales", ascending=False)
)

customers_sales_df.show()

spark.stop()
