@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================
REM CONFIG (modifiable)
REM ============================
set "KAFKA_CONTAINER=final_project-kafka-1"
set "SPARK_MASTER_CONTAINER=final_project-spark-master-1"
set "TOPIC=test_topic"

REM Spark job script inside spark-master (bind mount /opt/spark/work-dir)
set "SPARK_SCRIPT=/opt/spark/work-dir/spark_kafka_json_to_parquet_v2.py"

REM Parquet output/checkpoint inside container (bind mount to your ./data folder)
set "PARQUET_OUT=/opt/project_data/parquet_events_v2"
set "CKPT=/opt/project_data/checkpoints/parquet_events_v2"

REM Streamlit app path on Windows (relative to project root)
set "STREAMLIT_APP=src\dashboard\app.py"
set "STREAMLIT_PORT=8501"

REM Optional: clean checkpoint/metadata at each run (1=yes, 0=no)
set "CLEAN_RUN=0"

echo.
echo ==========================================
echo   RUN ALL - Start Docker + Kafka + Spark + UI
echo ==========================================
echo.

REM Go to this .bat location (project root)
cd /d "%~dp0"

REM ---- Check docker command
docker version >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Docker not running or not installed.
  echo - Start Docker Desktop, then re-run this script.
  exit /b 1
)

REM ---- Start docker compose
echo [1/7] Starting docker compose...
docker compose up -d
if errorlevel 1 (
  echo [ERROR] docker compose up failed.
  exit /b 1
)

REM ---- Wait containers
echo [2/7] Waiting containers to be ready...
set /a tries=0
:wait_loop
set /a tries+=1
docker ps --format "{{.Names}}" | findstr /i "%KAFKA_CONTAINER%" >nul
if errorlevel 1 (
  if !tries! GEQ 30 (
    echo [ERROR] Kafka container not found after waiting.
    echo Check: docker ps
    exit /b 1
  )
  timeout /t 2 >nul
  goto wait_loop
)

REM ---- Kafka quick health check
echo [3/7] Checking Kafka broker...
docker exec -it %KAFKA_CONTAINER% bash -lc "kafka-broker-api-versions --bootstrap-server kafka:29092 | head -n 2" >nul 2>&1
if errorlevel 1 (
  echo [WARN] Kafka broker check failed now. Waiting 5s and retry...
  timeout /t 5 >nul
  docker exec -it %KAFKA_CONTAINER% bash -lc "kafka-broker-api-versions --bootstrap-server kafka:29092 | head -n 2" >nul 2>&1
  if errorlevel 1 (
    echo [ERROR] Kafka still not responding.
    exit /b 1
  )
)

REM ---- Create topic if needed
echo [4/7] Ensuring topic "%TOPIC%" exists...
docker exec -it %KAFKA_CONTAINER% bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --topic %TOPIC% --partitions 1 --replication-factor 1 --if-not-exists" >nul 2>&1

REM ---- Optional clean run (checkpoint + _spark_metadata)
if "%CLEAN_RUN%"=="1" (
  echo [5/7] CLEAN_RUN=1 -> Removing checkpoint and Spark metadata...
  docker exec -it %SPARK_MASTER_CONTAINER% bash -lc "rm -rf %CKPT% %PARQUET_OUT%/_spark_metadata || true" >nul 2>&1
) else (
  echo [5/7] CLEAN_RUN=0 -> Keeping checkpoint/metadata.
)

REM ---- Start Spark job (Kafka -> Parquet) in background
echo [6/7] Starting Spark streaming job (Kafka -> Parquet)...
docker exec -it %SPARK_MASTER_CONTAINER% bash -lc "pkill -f spark_kafka_json_to_parquet_v2.py || true; pkill -f SparkSubmit || true" >nul 2>&1

docker exec -d %SPARK_MASTER_CONTAINER% bash -lc ^
"nohup env KAFKA_BOOTSTRAP=kafka:29092 KAFKA_TOPIC=%TOPIC% KAFKA_STARTING_OFFSETS=earliest ^
/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --master spark://spark-master:7077 ^
--repositories https://repo.maven.apache.org/maven2 ^
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 ^
%SPARK_SCRIPT% > /tmp/kafka_to_parquet.log 2>&1 & echo STARTED" >nul 2>&1

REM ---- Verify Spark job started
timeout /t 2 >nul
docker exec -it %SPARK_MASTER_CONTAINER% bash -lc "ps aux | grep -E 'SparkSubmit|spark_kafka_json_to_parquet_v2.py' | grep -v grep || true" > "%~dp0\spark_ps.txt"
findstr /i "spark_kafka_json_to_parquet_v2.py" "%~dp0\spark_ps.txt" >nul
if errorlevel 1 (
  echo [WARN] Spark job not detected yet. Check logs: docker exec -it %SPARK_MASTER_CONTAINER% bash -lc "tail -n 80 /tmp/kafka_to_parquet.log"
) else (
  echo [OK] Spark job is running.
)

REM ---- Start Streamlit in a new terminal window
echo [7/7] Starting Streamlit dashboard in a new window...
if not exist "%STREAMLIT_APP%" (
  echo [WARN] Streamlit app not found at "%STREAMLIT_APP%".
  echo Edit STREAMLIT_APP inside run_all.bat to correct path.
) else (
  start "Emotion Dashboard" cmd /k ^
  "cd /d ""%~dp0"" && python -m streamlit run ""%STREAMLIT_APP%"" --server.port %STREAMLIT_PORT%"
  timeout /t 2 >nul
  start "" "http://localhost:%STREAMLIT_PORT%"
)

echo.
echo ==========================================
echo   DONE. Services should be running.
echo - Kafka topic: %TOPIC%
echo - Spark log:  docker exec -it %SPARK_MASTER_CONTAINER% bash -lc "tail -n 80 /tmp/kafka_to_parquet.log"
echo - Parquet folder (Windows): .\data\parquet_events_v2
echo ==========================================
echo.

endlocal
exit /b 0