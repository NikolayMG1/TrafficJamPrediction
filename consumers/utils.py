import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType

def start_session(mongo_connection_string: str):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    spark = SparkSession.builder \
      .appName("TrafficFinlandConsumer") \
      .master("local[*]") \
      .config(
          "spark.jars.packages",
          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,"
          "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
      ) \
      .config("spark.mongodb.connection.uri", mongo_connection_string) \
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")  
    return spark


def read_and_parse_data(spark: SparkSession, kafka_topic: str, schema: StructType):
    df_raw = spark.readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
      .option("subscribe", kafka_topic) \
      .option("startingOffsets", "latest") \
      .load()

    df_str = df_raw.selectExpr("CAST(value AS STRING) as json_str")

    return df_str.select(
      from_json(col("json_str"), schema).alias("data")
      ).select("data.*")


def write_to_mongodb(df, checkpoint_location: str):
    query = df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", checkpoint_location) \
        .option("spark.mongodb.write.operationType", "insert") \
        .start()
        
    query.awaitTermination()