from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# 1. Start Spark session
spark = SparkSession.builder \
    .appName("DigitrafficRealTime") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema for Digitraffic station data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("avgSpeed", DoubleType(), True),
    StructField("vehicleCount", IntegerType(), True),
    StructField("direction", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
    # Add more fields if needed
])

# 3. Read from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "traffic-digitraffic") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka message value is in bytes, convert to string
df_str = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON using schema
df_parsed = df_str.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 4. Compute real-time metrics: average speed per station
df_metrics = df_parsed.groupBy("id", "name") \
    .agg(
        avg("avgSpeed").alias("avg_speed"),
        avg("vehicleCount").alias("avg_vehicle_count")
    )

# 5. Output to console for testing
query = df_metrics.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
