from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.sql.functions import from_json, col, explode
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

df = spark.read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'TfL-crowding') \
  .load()

df.printSchema()
df.show()

# convert the json into string from binary to make it accessible
json_df = df.select("value")
json_df = json_df.withColumn("json_string", json_df["value"].cast("string"))



json_schema = StructType([
                          StructField('naptan', StringType(), nullable=True),
                          StructField('daysOfWeek', ArrayType(
                              StructType([
                                  StructField('dayOfWeek', StringType(), nullable=True),
                                  StructField('amPeakTimeBand', StringType(), nullable=True),
                                  StructField('pmPeakTimeBand', StringType(), nullable=True),
                                  StructField('timeBands', ArrayType(
                                    StructType([
                                        StructField('timeBand', StringType(), nullable=True),
                                        StructField('percentageOfBaseLine', FloatType(), nullable=True)
                                    ])
                                  ))
                              ])))
])

# Parse the JSON data with the provided schema
json_df = json_df.withColumn("json_struct", from_json("json_string", json_schema))

# Extract the value of the stopPoints field
json_df = json_df.withColumn("daysOfWeek", col("json_struct.daysOfWeek"))

json_df.printSchema()
json_df.show()

days_of_week = json_df.select("daysOfWeek")
days_of_week.printSchema()
days_of_week.show()

# Accessing values from 'crowding_value' array
crowding_df = days_of_week.select(col("daysOfWeek.dayOfWeek").alias("dayOfWeek"),
                   col("daysOfWeek.amPeakTimeBand").alias("amPeakTimeBand"),
                   col("daysOfWeek.pmPeakTimeBand").alias("pmPeakTimeBand"),
                   col("daysOfWeek.timeBands").alias("timeBands")
                  )
crowding_df.show()

time_band_df = crowding_df.select(explode("timeBands").alias("timeBand_data"))

# Accessing the nested fields
time_band_df = time_band_df.select("timeBand_data.timeBand", "timeBand_data.percentageOfBaseLine")

time_band_df.show()
