from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySparkTest").getOrCreate()
data = [("Alice", 25), ("Bob", 30), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()