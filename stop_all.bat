@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================
REM CONFIG (modifiable)
REM ============================
set "SPARK_MASTER_CONTAINER=final_project-spark-master-1"
set "STREAMLIT_PORT=8501"

echo.
echo ==========================================
echo   STOP ALL - Stop UI + Spark + Docker
echo ==========================================
echo.

cd /d "%~dp0"

REM ---- 1) Stop Streamlit by killing the process listening on port 8501
echo [1/3] Stopping Streamlit (port %STREAMLIT_PORT%)...
for /f "tokens=5" %%P in ('netstat -ano ^| findstr :%STREAMLIT_PORT% ^| findstr LISTENING') do (
  echo   - Killing PID %%P
  taskkill /PID %%P /F >nul 2>&1
)

REM ---- 2) Stop Spark job inside container
echo [2/3] Stopping Spark job...
docker exec -it %SPARK_MASTER_CONTAINER% bash -lc "pkill -f spark_kafka_json_to_parquet_v2.py || true; pkill -f SparkSubmit || true" >nul 2>&1

REM ---- 3) Stop docker compose
echo [3/3] Stopping docker compose...
docker compose down
if errorlevel 1 (
  echo [WARN] docker compose down failed (maybe already stopped).
)

echo.
echo ==========================================
echo   DONE. Everything should be stopped.
echo ==========================================
echo.

endlocal
exit /b 0