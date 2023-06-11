from pyspark.sql import SparkSession

topic = 'TfL-bus-arrival'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

# Subscribe to 1 topic
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", topic) \
  .load()
# df.isStreaming()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df.show()
