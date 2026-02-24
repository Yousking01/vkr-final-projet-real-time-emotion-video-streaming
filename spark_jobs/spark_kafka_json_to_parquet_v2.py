from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    to_timestamp, from_unixtime, coalesce,
    date_trunc, to_date, when, greatest
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType
)
import os

# ---------------------------
# Config from environment
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test_topic")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")  # latest / earliest
SPARK_TRIGGER = os.getenv("SPARK_TRIGGER", "5 seconds")

OUT_PATH = os.getenv("OUT_PATH", "/opt/project_data/parquet_events_v2")
CKPT_PATH = os.getenv("CKPT_PATH", "/opt/project_data/checkpoints/parquet_events_v2")

print(
    "CONFIG:",
    "KAFKA_BOOTSTRAP=", KAFKA_BOOTSTRAP,
    "KAFKA_TOPIC=", KAFKA_TOPIC,
    "STARTING_OFFSETS=", KAFKA_STARTING_OFFSETS,
    "TRIGGER=", SPARK_TRIGGER,
    "OUT_PATH=", OUT_PATH,
    "CKPT_PATH=", CKPT_PATH,
    flush=True
)

spark = (
    SparkSession.builder
    .appName("KafkaJsonToParquetV2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("source_id", StringType(), True),
    StructField("frame_id", LongType(), True),
    StructField("face_id", LongType(), True),
    StructField("producer_ts", DoubleType(), True),  # epoch seconds (float) or ms
    StructField("emotion", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("bbox_x", IntegerType(), True),
    StructField("bbox_y", IntegerType(), True),
    StructField("bbox_w", IntegerType(), True),
    StructField("bbox_h", IntegerType(), True),
])

# ---------------------------
# Kafka stream (robuste)
# ---------------------------
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .option("failOnDataLoss", "false")  # IMPORTANT: continue même si offsets changent
    # Optionnel (évite gros batch si backlog) :
    # .option("maxOffsetsPerTrigger", "2000")
    .load()
)

json_df = raw.selectExpr("CAST(value AS STRING) AS value_str")

parsed = (
    json_df
    .withColumn("json", from_json(col("value_str"), schema))
    .select("json.*")
    # drop JSON invalides / incomplets
    .where(col("source_id").isNotNull() & col("emotion").isNotNull())
)

# ---------------------------
# timestamps (producer_ts ms/sec + event_ts)
# ---------------------------
producer_ts_raw = col("producer_ts").cast("double")

# Si producer_ts ressemble à du millisecondes (ex: 1700000000000), convertir en secondes
producer_ts_sec = when(producer_ts_raw > lit(1e12), producer_ts_raw / lit(1000.0)).otherwise(producer_ts_raw)

producer_event_ts = to_timestamp(from_unixtime(producer_ts_sec))

events = (
    parsed
    .withColumn("spark_ts", current_timestamp())
    .withColumn("producer_ts_sec", producer_ts_sec)
    .withColumn("event_ts", coalesce(producer_event_ts, col("spark_ts")))
    .withColumn("event_time", col("event_ts"))  # alias pour Streamlit
    .withColumn("event_minute", date_trunc("minute", col("event_time")))
    .withColumn("event_date", to_date(col("event_time")))
)

# ---------------------------
# metrics (latence non négative)
# ---------------------------
spark_ts_sec = col("spark_ts").cast("double")

events = events.withColumn(
    "e2e_latency_ms",
    coalesce(greatest(lit(0.0), (spark_ts_sec - col("producer_ts_sec")) * lit(1000.0)),
             lit(None).cast("double"))
)

events = events.withColumn("processing_time_ms", lit(None).cast("double"))

# ---------------------------
# output parquet (partition par date = lecture plus stable + rapide)
# ---------------------------
query = (
    events.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", OUT_PATH)
    .option("checkpointLocation", CKPT_PATH)
    .partitionBy("event_date")   # IMPORTANT: mieux pour analytics
    .trigger(processingTime=SPARK_TRIGGER)
    .start()
)

query.awaitTermination()