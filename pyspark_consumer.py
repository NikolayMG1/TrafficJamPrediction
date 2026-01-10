from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

from kafka_producer import KAFKA_TOPIC

spark = SparkSession.builder \
    .appName("DigitrafficRealTime") \
    .config("spark.jars.packages",  "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

sensor_event_schema = StructType([
    StructField("event_time", StringType(), True),     
    StructField("station_id", IntegerType(), True),

    StructField("entry_id", IntegerType(), True),
    StructField("station_id_sensor_value", IntegerType(), True),

    StructField("sensor_name", StringType(), True),
    StructField("sensor_short_name", StringType(), True),

    StructField("unit", StringType(), True),
    StructField("value", DoubleType(), True)
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_str = df_raw.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_str.select(from_json(col("json_str"), sensor_event_schema).alias("data")).select("data.*")

# 4. Compute real-time metrics: average speed per station
# df_metrics = df_parsed.groupBy("id", "name") \
#     .agg(
#         avg("avgSpeed").alias("avg_speed"),
#         avg("vehicleCount").alias("avg_vehicle_count")
#     )

# 5. Output to console for testing
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
