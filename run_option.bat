@echo off
setlocal EnableExtensions

cd /d "C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\Final_project"

echo ========================================
echo   NETTOYAGE DES DOSSIERS LOCAUX
echo ========================================
echo.

echo Suppression des anciens dossiers de donnees...
rmdir /s /q data\parquet_events_v2 2>nul
rmdir /s /q data\checkpoints\parquet_events_v2 2>nul
echo OK - Anciens dossiers supprimes
echo.

echo Creation des nouveaux dossiers...
mkdir data\parquet_events_v2 2>nul
mkdir data\checkpoints\parquet_events_v2 2>nul
echo OK - Nouveaux dossiers crees
echo.

echo ========================================
echo   LANCEMENT DU PROJET - DEMARRAGE
echo ========================================
echo.

echo [1/8] Activation de l'environnement virtuel...
call .venv\Scripts\activate.bat
if errorlevel 1 (
  echo ❌ ERREUR: activation venv impossible.
  pause
  exit /b 1
)
echo OK
echo.

echo [2/8] Lancement des conteneurs Docker...
docker compose up -d
if errorlevel 1 (
  echo ❌ ERREUR: docker compose up -d a echoue.
  pause
  exit /b 1
)
echo OK
echo.

echo [3/8] Attente de l'initialisation des conteneurs (15 secondes)...
timeout /t 15 /nobreak >nul
echo OK
echo.

echo [4/8] Verification des conteneurs en cours d'execution...
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo.
echo OK
echo.

echo [5/8] Verification de Kafka...
docker exec -it final_project-kafka-1 bash -lc "kafka-broker-api-versions --bootstrap-server kafka:29092 | head -n 5"
echo OK
echo.

echo [6/8] Configuration du topic Kafka (test_topic)...
docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --topic test_topic --partitions 1 --replication-factor 1 --if-not-exists"
echo.
echo Liste des topics disponibles :
docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --list"
echo OK
echo.

echo [7/8] Nettoyage des anciennes donnees Spark (dans le container)...
docker exec -it final_project-spark-master-1 bash -lc "rm -rf /opt/project_data/checkpoints/parquet_events_v2 /opt/project_data/parquet_events_v2 && mkdir -p /opt/project_data/checkpoints/parquet_events_v2 /opt/project_data/parquet_events_v2"
echo OK
echo.

echo [8/8] Lancement du job Spark Streaming (Kafka -> Parquet)...
docker exec -d final_project-spark-master-1 bash -lc "nohup env KAFKA_BOOTSTRAP=kafka:29092 KAFKA_TOPIC=test_topic STARTING_OFFSETS=earliest TRIGGER='5 seconds' OUT_PATH=/opt/project_data/parquet_events_v2 CKPT_PATH=/opt/project_data/checkpoints/parquet_events_v2 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.3 /opt/spark/work-dir/spark_kafka_json_to_parquet_v2.py > /tmp/kafka_to_parquet.log 2>&1 & echo STARTED"
echo OK
echo.

echo Verification du processus Spark (attente 5 secondes)...
timeout /t 5 /nobreak >nul
docker exec -it final_project-spark-master-1 bash -lc "ps aux | grep -E 'SparkSubmit|spark_kafka_json_to_parquet_v2.py|KafkaJsonToParquetV2' | grep -v grep"
echo.
echo OK
echo.

echo ========================================
echo   PROJET LANCE AVEC SUCCES !
echo ========================================
echo.
echo Commandes utiles :
echo ------------------
echo Logs Spark :
echo   docker exec -it final_project-spark-master-1 bash -lc "tail -f /tmp/kafka_to_parquet.log"
echo.
echo Envoyer 1 message test Kafka :
echo   docker exec -it final_project-kafka-1 bash -lc "printf '%%s\n' '{\"source_id\":\"cam_01\",\"frame_id\":1,\"face_id\":1,\"producer_ts\":'\"'\"'$(date +%%s.%%N)'\"'\"',\"emotion\":\"happy\",\"score\":0.92,\"bbox_x\":10,\"bbox_y\":20,\"bbox_w\":100,\"bbox_h\":120}' | kafka-console-producer --bootstrap-server kafka:29092 --topic test_topic"
echo.
echo Voir les fichiers Parquet :
echo   docker exec -it final_project-spark-master-1 bash -lc "find /opt/project_data/parquet_events_v2 -type f -name '*.parquet' | head -n 10"
echo.
echo Pour lancer le dashboard :
echo   streamlit run src\dashboard\app.py
echo.

echo Appuyez sur une touche pour lancer le dashboard maintenant...
pause >nul

echo.
echo Lancement du dashboard Streamlit...
start "Dashboard" cmd /k "cd /d C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\Final_project && call .venv\Scripts\activate.bat && streamlit run src\dashboard\app.py"

echo.
echo Dashboard lance dans une nouvelle fenetre !
echo.
pause