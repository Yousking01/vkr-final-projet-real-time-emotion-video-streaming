@echo off
cd /d "C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\Final_project"

echo ========================================
echo   ARRET DU PROJET
echo ========================================
echo.

:: Option 1: Arrêt complet avec nettoyage
echo Choisissez le mode d'arrêt :
echo.
echo 1. Arrêt simple (conteneurs + dashboard)
echo 2. Arrêt complet + nettoyage (conteneurs + dashboard + donnees locales)
echo 3. Arrêt complet + nettoyage + suppression des volumes Docker
echo 4. Quitter sans rien faire
echo.
set /p choix="Votre choix (1, 2, 3 ou 4) : "

if "%choix%"=="1" goto SIMPLE_STOP
if "%choix%"=="2" goto FULL_CLEAN
if "%choix%"=="3" goto VOLUME_CLEAN
if "%choix%"=="4" goto EOF
goto stop_projet

:SIMPLE_STOP
echo.
echo [1/4] Arret du dashboard Streamlit...

:: Méthode 1: Tuer par le nom de la fenêtre
echo   - Recherche des fenetres Streamlit...
for /f "tokens=2 delims=," %%a in ('tasklist /fi "windowtitle eq *streamlit*" /nh /fo csv 2^>nul') do (
    taskkill /F /PID %%a >nul 2>&1
    echo   - Processus Streamlit tue (PID: %%a)
)

:: Méthode 2: Tuer par le nom du processus et la ligne de commande
echo   - Recherche des processus Python lancant streamlit...
for /f "tokens=2 delims=," %%b in ('tasklist /fi "imagename eq python.exe" /v /fo csv 2^>nul ^| findstr /i "streamlit"') do (
    taskkill /F /PID %%b >nul 2>&1
    echo   - Processus Python/Streamlit tue (PID: %%b)
)

:: Méthode 3: Tuer tous les processus sur le port Streamlit (8501 par défaut)
echo   - Verification du port 8501 (Streamlit par defaut)...
for /f "tokens=5" %%c in ('netstat -ano ^| findstr :8501 ^| findstr LISTENING 2^>nul') do (
    taskkill /F /PID %%c >nul 2>&1
    echo   - Processus sur le port 8501 tue (PID: %%c)
)

echo OK
echo.

echo [2/4] Arret des conteneurs Docker...
docker compose down
echo OK
echo.

echo [3/4] Verification des processus restants...
docker ps -a --filter "name=final_project" --format "table {{.Names}}\t{{.Status}}"
echo.
echo OK

echo [4/4] Liberation des ressources...
echo.
echo ✅ Projet arrete avec succes !
echo.
goto END

:FULL_CLEAN
echo.
echo [1/5] Arret du dashboard Streamlit...

:: Méthode 1: Tuer par le nom de la fenêtre
for /f "tokens=2 delims=," %%a in ('tasklist /fi "windowtitle eq *streamlit*" /nh /fo csv 2^>nul') do (
    taskkill /F /PID %%a >nul 2>&1
    echo   - Processus Streamlit tue (PID: %%a)
)

:: Méthode 2: Tuer par le nom du processus et la ligne de commande
for /f "tokens=2 delims=," %%b in ('tasklist /fi "imagename eq python.exe" /v /fo csv 2^>nul ^| findstr /i "streamlit"') do (
    taskkill /F /PID %%b >nul 2>&1
    echo   - Processus Python/Streamlit tue (PID: %%b)
)

:: Méthode 3: Tuer tous les processus sur le port Streamlit
for /f "tokens=5" %%c in ('netstat -ano ^| findstr :8501 ^| findstr LISTENING 2^>nul') do (
    taskkill /F /PID %%c >nul 2>&1
    echo   - Processus sur le port 8501 tue (PID: %%c)
)

echo OK
echo.

echo [2/5] Arret des conteneurs Docker...
docker compose down
echo OK
echo.

echo [3/5] Suppression des donnees locales...
if exist "data\parquet_events_v2" (
    rmdir /s /q "data\parquet_events_v2"
    echo   - Supprime: data\parquet_events_v2
)
if exist "data\checkpoints\parquet_events_v2" (
    rmdir /s /q "data\checkpoints\parquet_events_v2"
    echo   - Supprime: data\checkpoints\parquet_events_v2
)
if exist "data" (
    rmdir /s /q "data" 2>nul
    echo   - Supprime: data
)
echo OK - Donnees locales supprimees
echo.

echo [4/5] Nettoyage des caches Spark dans les conteneurs...
docker exec final_project-spark-master-1 bash -lc "rm -rf /opt/project_data/checkpoints/* /opt/project_data/parquet_events_v2/* 2>/dev/null || true" >nul 2>&1
echo OK
echo.

echo [5/5] Liberation des ressources...
echo.
echo ✅ Projet arrete et nettoye avec succes !
echo.
goto END

:VOLUME_CLEAN
echo.
echo ⚠️  ATTENTION: Cette operation va supprimer TOUS les volumes Docker inutilises !
echo.
set /p confirm="Etes-vous sûr de vouloir continuer ? (O/N) : "

if /i not "!confirm!"=="O" goto END

echo.
echo [1/6] Arret du dashboard Streamlit...

:: Méthode 1: Tuer par le nom de la fenêtre
for /f "tokens=2 delims=," %%a in ('tasklist /fi "windowtitle eq *streamlit*" /nh /fo csv 2^>nul') do (
    taskkill /F /PID %%a >nul 2>&1
    echo   - Processus Streamlit tue (PID: %%a)
)

:: Méthode 2: Tuer par le nom du processus et la ligne de commande
for /f "tokens=2 delims=," %%b in ('tasklist /fi "imagename eq python.exe" /v /fo csv 2^>nul ^| findstr /i "streamlit"') do (
    taskkill /F /PID %%b >nul 2>&1
    echo   - Processus Python/Streamlit tue (PID: %%b)
)

:: Méthode 3: Tuer tous les processus sur le port Streamlit
for /f "tokens=5" %%c in ('netstat -ano ^| findstr :8501 ^| findstr LISTENING 2^>nul') do (
    taskkill /F /PID %%c >nul 2>&1
    echo   - Processus sur le port 8501 tue (PID: %%c)
)

echo OK
echo.

echo [2/6] Arret des conteneurs Docker...
docker compose down
echo OK
echo.

echo [3/6] Suppression des donnees locales...
if exist "data\parquet_events_v2" rmdir /s /q "data\parquet_events_v2"
if exist "data\checkpoints\parquet_events_v2" rmdir /s /q "data\checkpoints\parquet_events_v2"
if exist "data" rmdir /s /q "data" 2>nul
echo OK - Donnees locales supprimees
echo.

echo [4/6] Suppression des volumes Docker associes...
docker volume prune -f
echo OK
echo.

echo [5/6] Suppression des conteneurs arretes...
docker container prune -f
echo OK
echo.

echo [6/6] Liberation complete des ressources...
docker system df
echo.
echo ✅ Nettoyage complet termine avec succes !
echo.
goto END

:END
echo.
echo ========================================
echo   OPTIONS SUPPLEMENTAIRES
echo ========================================
echo.
echo 1. Relancer le projet maintenant
echo 2. Voir l'etat des conteneurs
echo 3. Quitter
echo.
set /p choix2="Votre choix (1, 2 ou 3) : "

if "%choix2%"=="1" (
    echo.
    echo Lancement du projet...
    call run_option.bat
    goto EOF
)
if "%choix2%"=="2" (
    echo.
    docker ps -a --filter "name=final_project"
    echo.
    pause
    goto END
)
if "%choix2%"=="3" goto EOF
goto END

:EOF
echo.
echo Appuyez sur une touche pour fermer cette fenetre.
pause >nul