from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, BooleanType, DoubleType
from pyspark.sql.functions import from_json, col, explode
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

# Subscribe to 1 topic
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'TfL-bus-arrival') \
  .load()

df.printSchema()
df.show()

# convert the json into string from binary to make it accessible
json_df = df.select("value")
json_df = json_df.withColumn("json_string", json_df["value"].cast("string"))

json_schema = StructType([
                          StructField('$type', StringType(), nullable=False),
                          StructField('centrePoint', StringType(), nullable=False),
                          StructField('stopPoints', ArrayType(
                              StructType([
                                  StructField('$type', StringType(), nullable=False),
                                  StructField('naptanId', StringType(), nullable=False),
                                  StructField('indicator', StringType(), nullable=False),
                                  StructField('stopLetter', StringType(), nullable=False),
                                  StructField('modes', ArrayType(StringType()), nullable=False),
                                  StructField('icsCode', StringType(), nullable=False),
                                  StructField('stopType', StringType(), nullable=False),
                                  StructField('stationNaptan', StringType(), nullable=False),
                                  StructField('lines', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=False),
                                          StructField('id', StringType(), nullable=False),
                                          StructField('name', StringType(), nullable=False),
                                          StructField('uri', StringType(), nullable=False),
                                          StructField('type', StringType(), nullable=False),
                                          StructField('crowding', StructType([
                                              StructField('$type', StringType(), nullable=False)
                                          ]), nullable=False),
                                          StructField('routeType', StringType(), nullable=False),
                                          StructField('status', StringType(), nullable=False)
                                      ])
                                  ), nullable=False),
                                  StructField('lineGroup', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=False),
                                          StructField('naptanIdReference', StringType(), nullable=False),
                                          StructField('stationAtcoCode', StringType(), nullable=False),
                                          StructField('lineIdentifier', ArrayType(
                                              StringType()), nullable=False)
                                      ])
                                  ), nullable=False),
                                  StructField('lineModeGroups', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=False),
                                          StructField('modeName', StringType(), nullable=False),
                                          StructField('lineIdentifier', ArrayType(
                                              StringType()), nullable=False)
                                      ])
                                  ), nullable=False),
                                  StructField('status', BooleanType(), nullable=False),
                                  StructField('id', StringType(), nullable=False),
                                  StructField('commonName', StringType(), nullable=False),
                                  StructField('distance', StringType(), nullable=False),
                                  StructField('placeType', StringType(), nullable=False),
                                  StructField('additionalProperties', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=True),
                                          StructField('category', StringType(), nullable=True),
                                          StructField('key', StringType(), nullable=True),
                                          StructField('sourceSystemKey', StringType(), nullable=True),
                                          StructField('value', StringType(), nullable=True)
                                      ])
                                  ), nullable=True),
                                  StructField('children', ArrayType(StringType()), nullable=False),
                                  StructField('lat', DoubleType(), nullable=False),
                                  StructField('lon', DoubleType(), nullable=False)
                              ])
                          ), nullable=False),
                          StructField('pageSize', StringType(), nullable=False),
                          StructField('total', StringType(), nullable=False),
                          StructField('page', StringType(), nullable=False)
                          ])

# Parse the JSON data with the provided schema
json_df = json_df.withColumn("json_struct", from_json("json_string", json_schema))

# Extract the value of the stopPoints field
json_df = json_df.withColumn("stopPoints_value", col("json_struct.stopPoints"))

json_df.printSchema()
json_df.show()

arrival_df = json_df.select("stopPoints_value")
arrival_df.printSchema()
arrival_df.show()

# Accessing values from 'stopPoints_value' array
arrival_df.select(col("stopPoints_value.$type").alias("stop_point_type"),
                  col("stopPoints_value.naptanId").alias("stop_point_naptanId"),
                  col("stopPoints_value.indicator").alias("stop_point_indicator")
                  ).show()

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
