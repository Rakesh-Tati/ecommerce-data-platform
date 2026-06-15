from pyspark.sql import session
from pyspark.sql.functions import max

spark = session.SparkSession.builder.appName("Test Spark").getOrCreate()

data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
