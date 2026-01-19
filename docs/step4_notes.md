# Step 4 — Kafka → Spark → Parquet

- Spark Structured Streaming reads Kafka topic `test_topic`.
- Writes Parquet to:
  - Windows mounted folder: `./data/parquet_windows` (inside container: `/opt/project_data/parquet_windows`)
  - Container local folder: `/tmp/parquet_container`
- Checkpoints stored in:
  - `./data/checkpoints/parquet_windows`
  - `/tmp/checkpoints/parquet_container`
