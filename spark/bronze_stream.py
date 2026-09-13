from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

KAFKA_BOOTSTRAP_SERVERS = "broker:19092"
KAFKA_TOPIC = "weather.raw"
BRONZE_PATH = "/data/bronze/weather"
CHECKPOINT_PATH = "/data/checkpoints/bronze-weather"

spark = (
    SparkSession.builder
    .appName("weather-bronze-stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
bronze_df = (
    kafka_df
    .select(
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    )
)


query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)
query.awaitTermination()