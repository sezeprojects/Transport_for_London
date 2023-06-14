from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

# # Subscribe to bus-arrival topic
# bus_arrival_df = spark \
#   .read \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("subscribe", 'TfL-bus-arrival') \
#   .load()
# # bus_arrival_df.isStreaming()
# bus_arrival_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# bus_arrival_df.show()

# Subscribe to crowding topic
schema = StructType([
    StructField("daysOfWeek", StringType(), True)
])

kafka_stream = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'TfL-crowding') \
  .load()
# crowding_df.isStreaming()
# crowding_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


text_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# query = text_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Wait for the query to finish
# query.awaitTermination()
# crowding_df.show()


