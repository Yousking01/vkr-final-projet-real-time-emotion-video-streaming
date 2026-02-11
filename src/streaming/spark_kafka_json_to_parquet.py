from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("KafkaJsonToParquet").getOrCreate()

schema = StructType([
    StructField("source_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("emotion", StringType(), True),
    StructField("score", DoubleType(), True),
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "test_topic")
    .option("startingOffsets", "latest")
    .load()
)

json_df = df.selectExpr("CAST(value AS STRING) as value_str")

events = (
    json_df
    .withColumn("json", from_json(col("value_str"), schema))
    .select("json.*")
)

# write to Windows-mounted data folder
query = (
    events.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/opt/project_data/parquet_events")
    .option("checkpointLocation", "/opt/project_data/checkpoints/parquet_events")
    .start()
)

query.awaitTermination()
