import os
import json
import torch
import timm

OUT_DIR = "artifacts_vit_emotions_8cls"
CKPT_PATH = os.path.join(OUT_DIR, "best_vit_8cls.pth")
ONNX_PATH = os.path.join(OUT_DIR, "emotion_vit_small_8cls.onnx")

MODEL_NAME = "vit_small_patch16_224"
NUM_CLASSES = 8

device = "cuda" if torch.cuda.is_available() else "cpu"

# Lire l'ordre des classes (pour être 100% sûr)
with open(os.path.join(OUT_DIR, "class_names.json"), "r", encoding="utf-8") as f:
    class_names = json.load(f)
print("Class order:", class_names)

model = timm.create_model(MODEL_NAME, pretrained=False, num_classes=NUM_CLASSES)
state = torch.load(CKPT_PATH, map_location="cpu", weights_only=True)
model.load_state_dict(state)
model.eval().to(device)

dummy = torch.randn(1, 3, 224, 224, device=device)

torch.onnx.export(
    model,
    dummy,
    ONNX_PATH,
    input_names=["input"],
    output_names=["logits"],
    dynamic_axes={"input": {0: "batch"}, "logits": {0: "batch"}},
    opset_version=17
)

print("✅ Exported ONNX:", ONNX_PATH)