@echo off
setlocal

cd /d "C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\Final_project"

docker ps

docker compose up -d

docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

docker exec -it final_project-kafka-1 bash -lc "kafka-broker-api-versions --bootstrap-server kafka:29092 | head -n 5"

docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --list"

docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --topic test_topic --partitions 1 --replication-factor 1 --if-not-exists"

docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --list"

docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --describe --topic test_topic"

docker exec -it final_project-kafka-1 bash -lc "echo '{\"hello\":\"world\"}' | kafka-console-producer --bootstrap-server kafka:29092 --topic test_topic"

docker exec -it final_project-kafka-1 bash -lc "kafka-console-consumer --bootstrap-server kafka:29092 --topic test_topic --from-beginning --max-messages 1 --timeout-ms 5000"

docker exec -it final_project-kafka-1 bash -lc "echo '{\"hello\":\"world\"}' | kafka-console-producer --bootstrap-server kafka:9092 --topic test_topic"

docker exec -it final_project-kafka-1 bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic test_topic --from-beginning --max-messages 1 --timeout-ms 5000"

docker exec -it final_project-spark-master-1 bash -lc "rm -rf /opt/project_data/checkpoints/parquet_events_v2 /opt/project_data/parquet_events_v2/_spark_metadata || true"

docker exec -d final_project-spark-master-1 bash -lc "nohup env KAFKA_BOOTSTRAP=kafka:29092 KAFKA_TOPIC=test_topic KAFKA_STARTING_OFFSETS=earliest /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --master spark://spark-master:7077 --repositories https://repo.maven.apache.org/maven2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark/work-dir/spark_kafka_json_to_parquet_v2.py > /tmp/kafka_to_parquet.log 2>&1 & echo STARTED"

docker exec -it final_project-spark-master-1 bash -lc "ps aux | grep -E 'SparkSubmit|spark_kafka_json_to_parquet_v2.py' | grep -v grep"

docker exec -it final_project-spark-master-1 bash -lc "tail -n 80 /tmp/kafka_to_parquet.log"

docker exec -it final_project-kafka-1 bash -lc "echo '{\"source_id\":\"cam_01\",\"frame_id\":1,\"face_id\":1,\"producer_ts\":'$(date +%s.%3N)',\"emotion\":\"happy\",\"score\":0.92,\"bbox_x\":10,\"bbox_y\":20,\"bbox_w\":100,\"bbox_h\":120}' | kafka-console-producer --bootstrap-server kafka:9092 --topic test_topic"

docker exec -it final_project-spark-master-1 bash -lc "find /opt/project_data/parquet_events_v2 -maxdepth 2 -type f -name '*.parquet' | head -n 10"

streamlit run src\dashboard\app.py

endlocal