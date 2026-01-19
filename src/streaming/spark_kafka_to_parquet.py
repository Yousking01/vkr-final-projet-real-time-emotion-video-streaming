from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()

# 1) Read Kafka stream
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "test_topic")
    .option("startingOffsets", "latest")
    .load()
)

# 2) Transform to a clean schema
events = (
    df.selectExpr("CAST(value AS STRING) AS message")
      .withColumn("event_time", current_timestamp())
)

# Output A: Parquet to Windows-mounted folder (persist on your PC)
query_windows = (
    events.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/opt/project_data/parquet_windows")
    .option("checkpointLocation", "/opt/project_data/checkpoints/parquet_windows")
    .start()
)

# Output B: Parquet inside container filesystem
query_container = (
    events.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/tmp/parquet_container")
    .option("checkpointLocation", "/tmp/checkpoints/parquet_container")
    .start()
)

query_windows.awaitTermination()
query_container.awaitTermination()
