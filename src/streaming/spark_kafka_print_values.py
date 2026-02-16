from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
         .appName("KafkaPrintValues")
         .getOrCreate())

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "test_topic")
      .option("startingOffsets", "earliest")
      .load())

out = df.select(col("value").cast("string").alias("json"))

query = (out.writeStream
         .format("console")
         .option("truncate", "false")
         .start())

query.awaitTermination()
