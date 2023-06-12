from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

# Subscribe to bus-arrival topic
bus_arrival_df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'TfL-bus-arrival') \
  .load()
# bus_arrival_df.isStreaming()
bus_arrival_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
bus_arrival_df.show()

# Subscribe to crowding topic
crowding_df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'TfL-crowding') \
  .load()
# crowding_df.isStreaming()
crowding_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
crowding_df.show()
