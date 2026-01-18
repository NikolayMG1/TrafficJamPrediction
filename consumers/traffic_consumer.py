from pyspark.sql.functions import  col, avg, count, window, to_timestamp, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from consumers.utils import read_and_parse_data, start_session, write_to_mongodb
from producers.kafka_producer import KAFKA_TOPIC

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

spark = start_session("mongodb://localhost:27017/digi-traffic.traffic-data")  
df_parsed = read_and_parse_data(spark, KAFKA_TOPIC, sensor_event_schema)

################## Data Cleaning ###################
# 1. Keep only rows where unit is km/h
df_parsed = df_parsed.filter(col("unit") == "km/h")

# 2. Convert 'value' column to double (invalid numbers become null)
df_parsed = df_parsed.withColumn("value", col("value").cast("double"))

# 3. Keep only rows with valid speed: not null, >=0, <150
df_parsed = df_parsed.filter((col("value").isNotNull()) & (col("value") >= 0) & (col("value") < 150))

# 4. Validate station IDs match
df_parsed = df_parsed.filter(col("station_id") == col("station_id_sensor_value"))

################### Data Aggregation ###################
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

write_to_mongodb(df_agg, "C:/tmp/checkpoints/traffic_consumer_v1")
