@echo off
cd /d "C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\Final_project"

echo ========================================
echo   NETTOYAGE DES DOSSIERS LOCAUX
echo ========================================
echo.

:: Nettoyage des dossiers de données
echo Suppression des anciens dossiers de données...
rmdir /s /q data\parquet_events_v2 2>nul
rmdir /s /q data\checkpoints\parquet_events_v2 2>nul
echo OK - Anciens dossiers supprimés
echo.

echo Création des nouveaux dossiers...
mkdir data\parquet_events_v2 2>nul
mkdir data\checkpoints\parquet_events_v2 2>nul
echo OK - Nouveaux dossiers créés
echo.

echo ========================================
echo   LANCEMENT DU PROJET - DEMARRAGE
echo ========================================
echo.

:: Activation de l'environnement virtuel
echo [1/8] Activation de l'environnement virtuel...
call .venv\Scripts\activate.bat
echo OK
echo.

:: Lancement des conteneurs Docker
echo [2/8] Lancement des conteneurs Docker...
docker compose up -d
echo OK
echo.

:: Attente que les conteneurs soient prêts
echo [3/8] Attente de l'initialisation des conteneurs (15 secondes)...
timeout /t 15 /nobreak >nul
echo OK
echo.

:: Vérification de l'état des conteneurs
echo [4/8] Vérification des conteneurs en cours d'exécution...
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo.
echo OK
echo.

:: Vérification des versions API Kafka
echo [5/8] Vérification de Kafka...
docker exec -it final_project-kafka-1 bash -lc "kafka-broker-api-versions --bootstrap-server kafka:29092 | head -n 5"
echo OK
echo.

:: Création du topic Kafka s'il n'existe pas
echo [6/8] Configuration du topic Kafka...
docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --topic test_topic --partitions 1 --replication-factor 1 --if-not-exists"
echo.
echo Liste des topics disponibles :
docker exec -it final_project-kafka-1 bash -lc "kafka-topics --bootstrap-server kafka:29092 --list"
echo OK
echo.

:: Nettoyage des anciennes données Spark
echo [7/8] Nettoyage des anciennes données Spark...
docker exec -it final_project-spark-master-1 bash -lc "rm -rf /opt/project_data/checkpoints/parquet_events_v2 /opt/project_data/parquet_events_v2/_spark_metadata || true"
echo OK
echo.

:: Lancement du job Spark
echo [8/8] Lancement du job Spark Streaming...
docker exec -d final_project-spark-master-1 bash -lc "nohup env KAFKA_BOOTSTRAP=kafka:29092 KAFKA_TOPIC=test_topic KAFKA_STARTING_OFFSETS=earliest /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --master spark://spark-master:7077 --repositories https://repo.maven.apache.org/maven2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark/work-dir/spark_kafka_json_to_parquet_v2.py > /tmp/kafka_to_parquet.log 2>&1 & echo STARTED"
echo OK
echo.

:: Vérification que Spark est bien lancé
echo Vérification du processus Spark :
timeout /t 5 /nobreak >nul
docker exec -it final_project-spark-master-1 bash -lc "ps aux | grep -E 'SparkSubmit|spark_kafka_json_to_parquet_v2.py' | grep -v grep"
echo.
echo OK
echo.

echo ========================================
echo   PROJET LANCE AVEC SUCCES !
echo ========================================
echo.
echo Commandes utiles :
echo ------------------
echo Pour voir les logs Spark : docker exec -it final_project-spark-master-1 bash -lc "tail -f /tmp/kafka_to_parquet.log"
echo Pour envoyer des donnees : docker exec -it final_project-kafka-1 bash -lc "echo '{\"source_id\":\"cam_01\",\"frame_id\":1,\"face_id\":1,\"producer_ts\":'$(date +%%s.%%3N)',\"emotion\":\"happy\",\"score\":0.92,\"bbox_x\":10,\"bbox_y\":20,\"bbox_w\":100,\"bbox_h\":120}' | kafka-console-producer --bootstrap-server kafka:29092 --topic test_topic"
echo Pour voir les fichiers Parquet generes : docker exec -it final_project-spark-master-1 bash -lc "find /opt/project_data/parquet_events_v2 -type f -name '*.parquet' | head -n 10"
echo Pour lancer le dashboard : streamlit run src\dashboard\app.py
echo.
echo Appuyez sur une touche pour lancer le dashboard maintenant...
pause >nul

:: Lancement du dashboard Streamlit
echo.
echo Lancement du dashboard Streamlit...
start cmd /k "cd /d C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\Final_project && call .venv\Scripts\activate.bat && streamlit run src\dashboard\app.py"

echo.
echo Dashboard lance dans une nouvelle fenetre !
echo.
pause