#Requires -Version 5.1
$ErrorActionPreference = "Stop"

# =========================
# Config
# =========================
$ModelsDir   = Join-Path $PSScriptRoot "models"
$ModelUrl    = "https://github.com/onnx/models/raw/main/vision/body_analysis/emotion_ferplus/model/emotion-ferplus-8.onnx"
$ModelPath   = Join-Path $ModelsDir "emotion_vit_small.onnx"   # nom local voulu
$EmotionsPath= Join-Path $ModelsDir "emotions.txt"

Write-Host "=== Setup des modèles ===" -ForegroundColor Cyan

try {
    # =========================
    # 1) Créer le dossier models
    # =========================
    if (-not (Test-Path $ModelsDir)) {
        New-Item -ItemType Directory -Path $ModelsDir -Force | Out-Null
        Write-Host "Dossier créé: $ModelsDir" -ForegroundColor Green
    } else {
        Write-Host "Dossier déjà présent: $ModelsDir" -ForegroundColor DarkGray
    }

    # =========================
    # 2) Nettoyer anciens fichiers (optionnel mais utile)
    # =========================
    Get-ChildItem -Path $ModelsDir -Filter "*.onnx" -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $ModelsDir -Filter "*.txt"  -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
    Write-Host "Nettoyage des anciens .onnx/.txt terminé" -ForegroundColor Yellow

    # =========================
    # 3) Télécharger le modèle
    # =========================
    Write-Host "Téléchargement du modèle FER..." -ForegroundColor Cyan

    # Astuce: certains environnements ont besoin de TLS 1.2
    try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

    Invoke-WebRequest -Uri $ModelUrl -OutFile $ModelPath -UseBasicParsing

    # =========================
    # 4) Vérifier le téléchargement
    # =========================
    if (-not (Test-Path $ModelPath)) {
        throw "Le fichier n'a pas été créé: $ModelPath"
    }

    $sizeBytes = (Get-Item $ModelPath).Length
    if ($sizeBytes -lt 1024) {
        throw "Fichier trop petit (probablement erreur de téléchargement): $sizeBytes octets"
    }

    $sizeMB = [math]::Round($sizeBytes / 1MB, 2)
    Write-Host "Modèle téléchargé: $sizeMB MB -> $ModelPath" -ForegroundColor Green

    # =========================
    # 5) Créer emotions.txt
    # =========================
    $emotions = @(
        "angry",
        "disgust",
        "fear",
        "happy",
        "sad",
        "surprise",
        "neutral"
    )

    # UTF-8 propre (Windows PowerShell peut être capricieux => utf8 est OK ici)
    $emotions -join "`r`n" | Out-File -FilePath $EmotionsPath -Encoding utf8 -Force

    Write-Host "Fichier emotions.txt créé -> $EmotionsPath" -ForegroundColor Green
    Write-Host "=== Terminé avec succès ===" -ForegroundColor Cyan
}
catch {
    Write-Host "ERREUR: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Astuce: vérifie ta connexion Internet / pare-feu / proxy, puis relance." -ForegroundColor DarkYellow
    exit 1
}