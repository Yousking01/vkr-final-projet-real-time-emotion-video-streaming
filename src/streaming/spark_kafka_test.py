from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaReadTest").getOrCreate()

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "test_topic")
    .option("startingOffsets", "earliest")
    .load()
)

out = df.selectExpr("CAST(value AS STRING) as message")

query = (
    out.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
