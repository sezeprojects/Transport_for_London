from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, ArrayType, FloatType, \
    BooleanType, \
    DoubleType
from pyspark.sql.functions import from_json, col, explode
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:" \
                                    "spark-streaming-kafka-0-10_2.12:3.2.0," \
                                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

# Subscribe to 1 topic
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'TfL-stop-points') \
  .load()

# df.printSchema()
# df.show()

# convert the json into string from binary to make it accessible
json_df = df.select("value")
json_df = json_df.withColumn("json_string", json_df["value"].cast("string"))

json_schema = StructType([
                          StructField('$type', StringType(), nullable=True),
                          StructField('centrePoint', StringType(), nullable=True),
                          StructField('stopPoints', ArrayType(
                              StructType([
                                  StructField('$type', StringType(), nullable=True),
                                  StructField('naptanId', StringType(), nullable=True),
                                  StructField('indicator', StringType(), nullable=True),
                                  StructField('stopLetter', StringType(), nullable=True),
                                  StructField('modes', ArrayType(StringType()), nullable=True),
                                  StructField('icsCode', StringType(), nullable=True),
                                  StructField('stopType', StringType(), nullable=True),
                                  StructField('stationNaptan', StringType(), nullable=True),
                                  StructField('lines', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=True),
                                          StructField('id', StringType(), nullable=True),
                                          StructField('name', StringType(), nullable=True),
                                          StructField('uri', StringType(), nullable=True),
                                          StructField('type', StringType(), nullable=True),
                                          StructField('crowding', StructType([
                                              StructField('$type', StringType(), nullable=True)
                                          ]), nullable=True),
                                          StructField('routeType', StringType(), nullable=True),
                                          StructField('status', StringType(), nullable=True)
                                      ])
                                  ), nullable=True),
                                  StructField('lineGroup', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=True),
                                          StructField('naptanIdReference', StringType(), nullable=True),
                                          StructField('stationAtcoCode', StringType(), nullable=True),
                                          StructField('lineIdentifier', ArrayType(
                                              StringType()), nullable=True)
                                      ])
                                  ), nullable=True),
                                  StructField('lineModeGroups', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=True),
                                          StructField('modeName', StringType(), nullable=True),
                                          StructField('lineIdentifier', ArrayType(
                                              StringType()), nullable=True)
                                      ])
                                  ), nullable=True),
                                  StructField('status', BooleanType(), nullable=True),
                                  StructField('id', StringType(), nullable=True),
                                  StructField('commonName', StringType(), nullable=True),
                                  StructField('distance', StringType(), nullable=True),
                                  StructField('placeType', StringType(), nullable=True),
                                  StructField('additionalProperties', ArrayType(
                                      StructType([
                                          StructField('$type', StringType(), nullable=True),
                                          StructField('category', StringType(), nullable=True),
                                          StructField('key', StringType(), nullable=True),
                                          StructField('sourceSystemKey', StringType(), nullable=True),
                                          StructField('value', StringType(), nullable=True)
                                      ])
                                  ), nullable=True),
                                  StructField('children', ArrayType(StringType()), nullable=True),
                                  StructField('lat', DoubleType(), nullable=True),
                                  StructField('lon', DoubleType(), nullable=True)
                              ])
                          ), nullable=True),
                          StructField('pageSize', StringType(), nullable=True),
                          StructField('total', StringType(), nullable=True),
                          StructField('page', StringType(), nullable=True)
                          ])

# Parse the JSON data with the provided schema
json_df = json_df.withColumn("json_struct", from_json("json_string", json_schema))

# Extract the value of the stopPoints field
json_df = json_df.withColumn("stopPoints_value", col("json_struct.stopPoints"))

# json_df.printSchema()
# json_df.show()

stop_points_df = json_df.select("stopPoints_value")
# arrival_df.printSchema()
# arrival_df.show()

# Accessing values from 'stopPoints_value' array
stop_points_df.select(col("stopPoints_value.$type").alias("stop_point_type"),
                      col("stopPoints_value.naptanId").alias("stop_point_naptanId"),
                      col("stopPoints_value.indicator").alias("stop_point_indicator")
                      )  # .show()

df = spark.read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'TfL-crowding') \
  .load()

# df.printSchema()
# df.show()

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

# json_df.printSchema()
# json_df.show()

days_of_week = json_df.select("daysOfWeek")
# days_of_week.printSchema()
# days_of_week.show()

# Accessing values from 'crowding_value' array
crowding_df = days_of_week.select(col("daysOfWeek.dayOfWeek").alias("dayOfWeek"),
                                  col("daysOfWeek.amPeakTimeBand").alias("amPeakTimeBand"),
                                  col("daysOfWeek.pmPeakTimeBand").alias("pmPeakTimeBand"),
                                  col("daysOfWeek.timeBands").alias("timeBands")
                                  )
# crowding_df.show()

time_band_df = crowding_df.select(explode("timeBands").alias("timeBand_data"))

# Accessing the nested fields
time_band_df = time_band_df.select("timeBand_data.timeBand", "timeBand_data.percentageOfBaseLine")

# time_band_df.show()

df = spark.read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", 'stop-DE') \
  .load()

# df.printSchema()
# df.show()

# convert the json into string from binary to make it accessible
json_df = df.select("value")
json_df = json_df.withColumn("json_string", json_df["value"].cast("string"))

json_schema = StructType([
                          StructField('$type', StringType(), nullable=True),
                          StructField('id', StringType(), nullable=True),
                          StructField('operationType', IntegerType(), nullable=True),
                          StructField('vehicleId', StringType(), nullable=True),
                          StructField('naptanId', StringType(), nullable=True),
                          StructField('stationName', StringType(), nullable=True),
                          StructField('lineId', StringType(), nullable=True),
                          StructField('lineName', StringType(), nullable=True),
                          StructField('platformName', StringType(), nullable=True),
                          StructField('direction', StringType(), nullable=True),
                          StructField('bearing', StringType(), nullable=True),
                          StructField('destinationNaptanId', StringType(), nullable=True),
                          StructField('destinationName', StringType(), nullable=True),
                          StructField('timestamp', StringType(), nullable=True),
                          StructField('timeToStation', IntegerType(), nullable=True),
                          StructField('currentLocation', StringType(), nullable=True),
                          StructField('towards', StringType(), nullable=True),
                          StructField('expectedArrival', StringType(), nullable=True),
                          StructField('timeToLive', StringType(), nullable=True),
                          StructField('modeName', StringType(), nullable=True),
                          StructField('timing', StructType([
                              StructField('$type', StringType(), nullable=True),
                              StructField('countdownServerAdjustment', StringType(), nullable=True),
                              StructField('source', StringType(), nullable=True),
                              StructField('insert', StringType(), nullable=True),
                              StructField('read', StringType(), nullable=True),
                              StructField('sent', StringType(), nullable=True),
                              StructField('received', StringType(), nullable=True)
                          ]))
                      ])

# Parse the array string and extract its element as a column
json_array_df = json_df.withColumn("json_array", from_json(col("json_string"), "array<string>"))

# json_array_df.printSchema()
# json_array_df.show()

json_exploded_df = json_array_df.withColumn("exploded_array", explode(col("json_array")))
# json_exploded_df.printSchema()
# json_exploded_df.show()

# Parse the JSON data with the provided schema
json_struct_df = json_exploded_df.withColumn("json_struct", from_json("exploded_array", json_schema))
json_struct_df.printSchema()
# json_struct_df.show()

json_struct_df = json_struct_df.drop("value", "json_string", "json_array", "exploded_array")
json_struct_df.show()

arrival_info_df = json_struct_df.select("json_struct.stationName")
arrival_info_df.show()
