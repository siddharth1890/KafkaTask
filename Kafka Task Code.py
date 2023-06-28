######## Source code for Kafka Streaming Data ########

# Project Title:
# Author: 
# Designation:
# Organization:
# Version:
# Code_Name:
# Date Created:
# Date Deployed:
# Repository_name: 


# Import all libraries required 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Step 1: Create a Spark session
spark = SparkSession.builder.appName("ClickstreamDataPipeline").getOrCreate()

# Step 2: Define the schema for clickstream data
clickstream_schema = StructType([
    StructField("row_key", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("url", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("device", StringType(), True)
])

# Step 3: Read clickstream data from Kafka
clickstream_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "<kafka-bootstrap-servers>") \
    .option("subscribe", "<kafka-topic>") \
    .option("startingOffsets", "latest") \
    .load()

# Step 4: Parse the clickstream data from Kafka using the defined schema
clickstream_df = clickstream_data \
    .select(from_json(col("value").cast("string"), clickstream_schema).alias("data")) \
    .select("data.*")

# Step 5: Write the ingested data to AWS S3 in Parquet format
clickstream_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "<s3-checkpoint-location>") \
    .start("<s3-output-location>")

# Step 6: Process the stored clickstream data in AWS S3
processed_data = clickstream_df \
    .groupBy("url", "country") \
    .agg(
        countDistinct("user_id").alias("unique_users"),
        count("row_key").alias("clicks"),
        avg("timestamp").alias("avg_time_spent")
    )

# Step 7: Index the processed data in Elasticsearch
processed_data.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "<checkpoint-location>") \
    .option("es.nodes", "<elasticsearch-nodes>") \
    .option("es.port", "<elasticsearch-port>") \
    .option("es.resource", "<elasticsearch-resource>") \
    .option("es.mapping.id", "row_key") \
    .start()
