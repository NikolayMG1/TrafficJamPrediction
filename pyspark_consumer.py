from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, window, to_timestamp, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from kafka_producer import KAFKA_TOPIC

spark = SparkSession.builder \
    .appName("DigitrafficRealTime") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
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

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Cast value as string
df_str = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON
df_parsed = df_str.select(
    from_json(col("json_str"), sensor_event_schema).alias("data")
).select("data.*")

######## Data Cleaning #########

# 1. Keep only rows where unit is km/h
df_parsed = df_parsed.filter(col("unit") == "km/h")

# 2. Convert 'value' column to double (invalid numbers become null)
df_parsed = df_parsed.withColumn("value", col("value").cast("double"))

# 3. Keep only rows with valid speed: not null, >=0, <150
df_parsed = df_parsed.filter((col("value").isNotNull()) & (col("value") >= 0) & (col("value") < 150))

# 4. Validate station IDs match
df_parsed = df_parsed.filter(col("station_id") == col("station_id_sensor_value"))


########## Data Transformations ##########
df_parsed = df_parsed.withColumn("event_time_ts", to_timestamp("event_time"))

df_agg = df_parsed \
    .withWatermark("event_time_ts", "1 minute") \
    .groupBy(
        window(col("event_time_ts"), "5 minutes", "1 minute"),
        col("station_id")
    ).agg(
        count("entry_id").alias("vehicle_count"),
        avg("value").alias("avg_speed")
    )

df_agg = df_agg.withColumn(
    "congestion_state",
    when((col("avg_speed") > 60) | (col("vehicle_count") < 50), "Not congested")
    .when(((col("avg_speed") <= 60) & (col("avg_speed") > 40)) | ((col("vehicle_count") >= 50) & (col("vehicle_count") <= 100)), "Slightly congested")
    .when(((col("avg_speed") <= 40) & (col("avg_speed") > 20)) | ((col("vehicle_count") > 100) & (col("vehicle_count") <= 200)), "Heavy traffic")
    .otherwise("Full stop")
)

query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
