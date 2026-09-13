from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_PATH = "/data/bronze/weather"
SILVER_PATH = "/data/silver/weather"
CHECKPOINT_PATH = "/data/checkpoints/silver-weather"

spark = (
    SparkSession.builder
    .appName("weather-silver-stream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze_schema = StructType(
    [
        StructField("event_key", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("city", StringType(), False),
        StructField("display_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("observed_at", StringType(), False),
        StructField("collected_at", StringType(), False),
        StructField("temperature_c", DoubleType(), True),
        StructField("relative_humidity_pct", DoubleType(), True),
        StructField("apparent_temperature_c", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("rain_mm", DoubleType(), True),
        StructField("weather_code", DoubleType(), True),
        StructField("cloud_cover_pct", DoubleType(), True),
        StructField("surface_pressure_hpa", DoubleType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_direction_deg", DoubleType(), True),
    ]
)

bronze_df = (
    spark.readStream
    .schema(bronze_schema)
    .parquet(BRONZE_PATH)
)

parsed_df = (
    bronze_df
    .withColumn(
        "event",
        from_json(col("raw_payload"), event_schema),
    )
    .select("event.*")
    .withColumn(
        "observed_at",
        to_timestamp("observed_at"),
    )
    .withColumn(
        "collected_at",
        to_timestamp("collected_at"),
    )
    .withColumn(
        "observation_date",
        to_date("observed_at"),
    )
)

clean_df = (
    parsed_df
    .filter(col("event_id").isNotNull())
    .filter(col("city").isNotNull())
    .filter(col("observed_at").isNotNull())
    .withWatermark("observed_at", "30 minutes")
    .dropDuplicates(["event_id"])
)

query = (
    clean_df.writeStream
    .format("parquet")
    .outputMode("append")
    .partitionBy("observation_date")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)
query.awaitTermination()