from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from consumers.utils import read_and_parse_data, start_session, write_to_mongodb
from producers.kafka_producer import KAFKA_TOPIC_STATION

station_schema = StructType([
    StructField("station_id", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("altitude", DoubleType()),
])

spark = start_session("mongodb://localhost:27017/digi-traffic.stations")  
df_parsed = read_and_parse_data(spark, KAFKA_TOPIC_STATION, station_schema)
write_to_mongodb(df_parsed, "C:/tmp/checkpoint_station")
