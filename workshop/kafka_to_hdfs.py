from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaStream") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4") \
    .getOrCreate()

# Match the producer payload
schema = StructType([
    StructField('timestamp', StringType(), False),  # was DoubleType
    StructField('sensor_id', StringType(), False),
    StructField('value', DoubleType(), False)
])

# Read from the correct topic
df = spark.readStream.format("kafka") \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'sensor_data') \
    .option('startingOffsets', 'latest') \
    .option('failOnDataLoss', 'false') \
    .load() \
    .selectExpr("CAST(value AS STRING) AS json")

data = df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# Parse timestamp to real TimestampType (optional but useful)
data = data.withColumn(
    "event_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)

clean_data = data.filter(
    col("timestamp").isNotNull() &
    col("sensor_id").isNotNull() &
    col("value").isNotNull()
)

# Debug to console
console_q = clean_data.writeStream \
    .format('console') \
    .option('truncate', False) \
    .option('numRows', 10) \
    .start()

# Write to HDFS (ensure HDFS is up) — or switch to a local folder for a quick test
parquet_q = clean_data.writeStream \
    .format('parquet') \
    .option('path', 'hdfs://localhost:9000/Sensors') \
    .option('checkpointLocation', 'hdfs://localhost:9000/tmp/sensor_ckpt') \
    .outputMode('append') \
    .trigger(processingTime="30 seconds") \
    .start()

spark.streams.awaitAnyTermination()
