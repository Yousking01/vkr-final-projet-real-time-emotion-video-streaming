# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, from_json, current_timestamp, lit,
#     to_timestamp, from_unixtime, coalesce, greatest
# )
# from pyspark.sql.types import (
#     StructType, StructField, StringType, DoubleType,
#     LongType, IntegerType
# )

# # ---------------------------
# # Spark session
# # ---------------------------
# spark = (
#     SparkSession.builder
#     .appName("KafkaJsonToParquetV2")
#     .getOrCreate()
# )
# spark.sparkContext.setLogLevel("WARN")

# # ---------------------------
# # Schema (JSON value)
# # ---------------------------
# schema = StructType([
#     StructField("source_id", StringType(), True),
#     StructField("frame_id", LongType(), True),
#     StructField("face_id", LongType(), True),
#     StructField("producer_ts", DoubleType(), True),  # epoch seconds (float)
#     StructField("emotion", StringType(), True),
#     StructField("score", DoubleType(), True),
#     StructField("bbox_x", IntegerType(), True),
#     StructField("bbox_y", IntegerType(), True),
#     StructField("bbox_w", IntegerType(), True),
#     StructField("bbox_h", IntegerType(), True),
# ])

# # ---------------------------
# # Kafka stream
# # ---------------------------
# raw = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "kafka:29092")
#     .option("subscribe", "test_topic")
#     .option("startingOffsets", "latest")  # IMPORTANT: une seule fois
#     .load()
# )

# json_df = raw.selectExpr("CAST(value AS STRING) AS value_str")

# parsed = (
#     json_df
#     .withColumn("json", from_json(col("value_str"), schema))
#     .select("json.*")
# )

# # Si JSON invalide => json.* = null partout. On filtre.
# parsed = parsed.filter(col("source_id").isNotNull())

# # ---------------------------
# # Timestamps
# # ---------------------------
# # producer_ts (seconds epoch) -> timestamp. Si producer_ts est null => null.
# producer_event_ts = to_timestamp(from_unixtime(col("producer_ts")))

# events = (
#     parsed
#     .withColumn("spark_ts", current_timestamp())
#     # event_ts = producer_ts converti si possible, sinon spark_ts
#     .withColumn("event_ts", coalesce(producer_event_ts, col("spark_ts")))
#     # alias pour dashboard
#     .withColumn("event_time", col("event_ts"))
# )

# # ---------------------------
# # Metrics
# # ---------------------------
# # Latence brute en ms (peut être négative si clock skew)
# events = events.withColumn(
#     "e2e_latency_ms_raw",
#     (col("spark_ts").cast("double") - col("producer_ts")) * lit(1000.0)
# )

# # clamp: jamais négatif ; si producer_ts null => raw null => greatest(0, null) = 0
# events = events.withColumn(
#     "e2e_latency_ms",
#     greatest(lit(0.0), col("e2e_latency_ms_raw"))
# ).drop("e2e_latency_ms_raw")

# # Placeholder
# events = events.withColumn("processing_time_ms", lit(None).cast("double"))

# # ---------------------------
# # Output
# # ---------------------------
# OUT_PATH = "/opt/project_data/parquet_events_v2"
# # CKPT_PATH = "/opt/project_data/checkpoints/parquet_events_v2"
# CKPT_PATH = "/opt/project_data/checkpoints/parquet_events_v2_run2"


# query = (
#     events.writeStream
#     .format("parquet")
#     .outputMode("append")
#     .option("path", OUT_PATH)
#     .option("checkpointLocation", CKPT_PATH)
#     .trigger(processingTime="5 seconds")
#     .start()
# )

# query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, from_json, current_timestamp, lit,
#     to_timestamp, from_unixtime, coalesce, when
# )
# from pyspark.sql.types import (
#     StructType, StructField, StringType, DoubleType,
#     LongType, IntegerType
# )

# spark = (
#     SparkSession.builder
#     .appName("KafkaJsonToParquetV2")
#     .getOrCreate()
# )
# spark.sparkContext.setLogLevel("WARN")

# schema = StructType([
#     StructField("source_id", StringType(), True),
#     StructField("frame_id", LongType(), True),
#     StructField("face_id", LongType(), True),
#     StructField("producer_ts", DoubleType(), True),  # epoch seconds (float)
#     StructField("emotion", StringType(), True),
#     StructField("score", DoubleType(), True),
#     StructField("bbox_x", IntegerType(), True),
#     StructField("bbox_y", IntegerType(), True),
#     StructField("bbox_w", IntegerType(), True),
#     StructField("bbox_h", IntegerType(), True),
# ])

# # Kafka stream
# raw = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "kafka:29092")
#     .option("subscribe", "test_topic")
#     .option("startingOffsets", "latest")   # IMPORTANT: garder seulement les nouveaux
#     .load()
# )

# json_df = raw.selectExpr("CAST(value AS STRING) AS value_str")

# parsed = (
#     json_df
#     .withColumn("json", from_json(col("value_str"), schema))
#     .select("json.*")
#     # Drop messages invalides (json null / champs clés manquants)
#     .where(col("source_id").isNotNull() & col("emotion").isNotNull())
# )

# # --- timestamps ---
# # Normaliser producer_ts : si jamais ms -> sec
# producer_ts_sec = when(col("producer_ts") > lit(1e12), col("producer_ts") / lit(1000.0)).otherwise(col("producer_ts"))

# producer_event_ts = to_timestamp(from_unixtime(producer_ts_sec))

# events = (
#     parsed
#     .withColumn("spark_ts", current_timestamp())
#     .withColumn("producer_ts_sec", producer_ts_sec)
#     # event_ts = producer_ts si possible sinon spark_ts
#     .withColumn("event_ts", coalesce(producer_event_ts, col("spark_ts")))
#     .withColumn("event_time", col("event_ts"))  # alias pour dashboard
# )

# # --- metrics ---
# # e2e_latency_ms = (spark_ts - producer_ts_sec) * 1000
# events = events.withColumn(
#     "e2e_latency_ms",
#     (col("spark_ts").cast("double") - col("producer_ts_sec")) * lit(1000.0)
# )

# events = events.withColumn("processing_time_ms", lit(None).cast("double"))

# # --- output ---
# OUT_PATH = "/opt/project_data/parquet_events_v2"
# CKPT_PATH = "/opt/project_data/checkpoints/parquet_events_v2"

# query = (
#     events.writeStream
#     .format("parquet")
#     .outputMode("append")
#     .option("path", OUT_PATH)
#     .option("checkpointLocation", CKPT_PATH)
#     .trigger(processingTime="5 seconds")
#     .start()
# )

# query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, from_json, current_timestamp, lit,
#     to_timestamp, from_unixtime, coalesce,
#     date_trunc, to_date
# )
# from pyspark.sql.types import (
#     StructType, StructField, StringType, DoubleType,
#     LongType, IntegerType
# )
# import os

# # ---------------------------
# # Config from environment
# # ---------------------------
# KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test_topic")
# KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

# SPARK_TRIGGER = os.getenv("SPARK_TRIGGER", "10 seconds")

# OUT_PATH = os.getenv("OUT_PATH", "/opt/project_data/parquet_events_v2")
# CKPT_PATH = os.getenv("CKPT_PATH", "/opt/project_data/checkpoints/parquet_events_v2")

# print(
#     "CONFIG:",
#     "KAFKA_BOOTSTRAP=", KAFKA_BOOTSTRAP,
#     "KAFKA_TOPIC=", KAFKA_TOPIC,
#     "STARTING_OFFSETS=", KAFKA_STARTING_OFFSETS,
#     "TRIGGER=", SPARK_TRIGGER,
#     "OUT_PATH=", OUT_PATH,
#     "CKPT_PATH=", CKPT_PATH,
#     flush=True
# )

# spark = (
#     SparkSession.builder
#     .appName("KafkaJsonToParquetV2")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# schema = StructType([
#     StructField("source_id", StringType(), True),
#     StructField("frame_id", LongType(), True),
#     StructField("face_id", LongType(), True),
#     StructField("producer_ts", DoubleType(), True),  # epoch seconds (float)
#     StructField("emotion", StringType(), True),
#     StructField("score", DoubleType(), True),
#     StructField("bbox_x", IntegerType(), True),
#     StructField("bbox_y", IntegerType(), True),
#     StructField("bbox_w", IntegerType(), True),
#     StructField("bbox_h", IntegerType(), True),
# ])

# # Kafka stream
# raw = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#     .option("subscribe", KAFKA_TOPIC)
#     .option("startingOffsets", KAFKA_STARTING_OFFSETS)
#     .load()
# )

# json_df = raw.selectExpr("CAST(value AS STRING) AS value_str")

# parsed = (
#     json_df
#     .withColumn("json", from_json(col("value_str"), schema))
#     .select("json.*")
# )

# # --- timestamps ---
# # producer_ts (epoch seconds float) -> timestamp
# producer_event_ts = to_timestamp(from_unixtime(col("producer_ts")))

# # events = (
# #     parsed
# #     .withColumn("spark_ts", current_timestamp())
# #     .withColumn("event_ts", coalesce(producer_event_ts, col("spark_ts")))
# #     .withColumn("event_time", col("event_ts"))  # alias pour dashboard
# # )
# events = (
#     parsed
#     .withColumn("spark_ts", current_timestamp())
#     .withColumn("event_ts", coalesce(producer_event_ts, col("spark_ts")))
#     .withColumn("event_time", col("event_ts"))  # alias pour dashboard
#     .withColumn("event_minute", date_trunc("minute", col("event_time")))  # NEW
#     .withColumn("event_date", to_date(col("event_time")))                # NEW
# )

# # --- metrics ---
# events = events.withColumn(
#     "e2e_latency_ms",
#     (col("spark_ts").cast("double") - col("producer_ts")) * lit(1000.0)
# )

# events = events.withColumn("processing_time_ms", lit(None).cast("double"))

# # --- output ---
# query = (
#     events.writeStream
#     .format("parquet")
#     .outputMode("append")
#     .option("path", OUT_PATH)
#     .option("checkpointLocation", CKPT_PATH)
#     .trigger(processingTime=SPARK_TRIGGER)
#     .start()
# )

# query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    to_timestamp, from_unixtime, coalesce,
    date_trunc, to_date
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
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

SPARK_TRIGGER = os.getenv("SPARK_TRIGGER", "10 seconds")

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
    StructField("producer_ts", DoubleType(), True),  # epoch seconds (float)
    StructField("emotion", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("bbox_x", IntegerType(), True),
    StructField("bbox_y", IntegerType(), True),
    StructField("bbox_w", IntegerType(), True),
    StructField("bbox_h", IntegerType(), True),
])

# ---------------------------
# Kafka stream (IMPORTANT: failOnDataLoss=false)
# ---------------------------
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .option("failOnDataLoss", "false")  # ✅ évite crash si offsets changent
    .load()
)

json_df = raw.selectExpr("CAST(value AS STRING) AS value_str")

parsed = (
    json_df
    .withColumn("json", from_json(col("value_str"), schema))
    .select("json.*")
)

# ---------------------------
# timestamps
# ---------------------------
# producer_ts (epoch seconds float) -> timestamp (OK pour une démo)
producer_event_ts = to_timestamp(from_unixtime(col("producer_ts")))

events = (
    parsed
    .withColumn("spark_ts", current_timestamp())
    .withColumn("event_ts", coalesce(producer_event_ts, col("spark_ts")))
    .withColumn("event_time", col("event_ts"))  # alias dashboard
    .withColumn("event_minute", date_trunc("minute", col("event_time")))
    .withColumn("event_date", to_date(col("event_time")))
)

# ---------------------------
# metrics (robuste)
# ---------------------------
# spark_ts cast double = epoch seconds
spark_ts_sec = col("spark_ts").cast("double")
producer_ts_sec = col("producer_ts").cast("double")

events = events.withColumn(
    "e2e_latency_ms",
    coalesce((spark_ts_sec - producer_ts_sec) * lit(1000.0), lit(None).cast("double"))
)

# si tu n'as pas de processing_time réel, laisse NULL
events = events.withColumn("processing_time_ms", lit(None).cast("double"))

# ---------------------------
# output
# ---------------------------
query = (
    events.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", OUT_PATH)
    .option("checkpointLocation", CKPT_PATH)
    .trigger(processingTime=SPARK_TRIGGER)
    .start()
)

query.awaitTermination()
