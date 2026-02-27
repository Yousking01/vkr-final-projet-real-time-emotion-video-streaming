import os
from pathlib import Path
import torch
from transformers import AutoImageProcessor, AutoModelForImageClassification

MODEL_ID = "imfarzanansari/face-emotion-recognition"

ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = ROOT / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

ONNX_OUT = MODELS_DIR / "emotion_vit_small.onnx"
LABELS_OUT = MODELS_DIR / "emotions.txt"

def main():
    print("Téléchargement du modèle:", MODEL_ID)
    processor = AutoImageProcessor.from_pretrained(MODEL_ID)
    model = AutoModelForImageClassification.from_pretrained(MODEL_ID)
    model.eval()

    # Créer une entrée factice pour l'export
    dummy = torch.randn(1, 3, 224, 224, dtype=torch.float32)

    # Exporter en ONNX
    print("Exportation vers:", ONNX_OUT)
    torch.onnx.export(
        model,
        (dummy,),
        str(ONNX_OUT),
        input_names=["pixel_values"],
        output_names=["logits"],
        dynamic_axes={"pixel_values": {0: "batch"}, "logits": {0: "batch"}},
        opset_version=17,
    )

    # Récupérer les labels
    if hasattr(model.config, "id2label") and model.config.id2label:
        labels = [model.config.id2label[i] for i in range(len(model.config.id2label))]
    else:
        # Labels par défaut pour la reconnaissance d'émotions
        labels = ["angry", "disgust", "fear", "happy", "sad", "surprise", "neutral"]
    
    # Sauvegarder les labels
    LABELS_OUT.write_text("\n".join(labels) + "\n", encoding="utf-8")
    
    print(f"Export terminé!")
    print(f"   - Modèle: {ONNX_OUT} ({ONNX_OUT.stat().st_size / 1024 / 1024:.1f} MB)")
    print(f"   - Labels: {LABELS_OUT} ({len(labels)} classes)")

if __name__ == "__main__":
    main()