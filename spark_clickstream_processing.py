# Data Processing with Apache Spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("ClickstreamProcessor") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# Define schema for clickstream data
schema = StructType() \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("action", StringType()) \
    .add("timestamp", StringType())

# Read from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream-topic") \
    .load()

# Parse Kafka data as JSON
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Aggregate the data
agg_df = json_df.groupBy("product_id").count()

# Write the aggregated data to Cassandra
agg_df.writeStream \
    .outputMode("complete") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "clickstream") \
    .option("table", "product_clicks") \
    .option("checkpointLocation", "/tmp/clickstream-checkpoint") \
    .start() \
    .awaitTermination()
