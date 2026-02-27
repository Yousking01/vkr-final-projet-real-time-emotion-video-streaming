import onnxruntime as ort
import numpy as np
import time
from pathlib import Path

def _prod(shape):
    n = 1
    for x in shape:
        n *= int(x)
    return n

def _make_dummy(shape, dtype=np.float32):
    """
    Crée un input factice compatible avec le modèle.
    - Remplace les dimensions dynamiques/None par des valeurs par défaut.
    - Remplace les dimensions 'N' par 1.
    """
    fixed = []
    for i, d in enumerate(shape):
        if d is None:
            fixed.append(1)
        elif isinstance(d, str):
            fixed.append(1)
        else:
            fixed.append(int(d))

    # Si l'input ressemble à une image (N,C,H,W), on met des valeurs "classiques"
    # (ça ne change rien pour un dummy, mais aide si le modèle a des dims dynamiques)
    if len(fixed) == 4:
        n, c, h, w = fixed
        fixed[0] = 1 if n <= 0 else fixed[0]
        fixed[1] = 1 if c <= 0 else fixed[1]
        fixed[2] = 64 if h <= 0 else fixed[2]
        fixed[3] = 64 if w <= 0 else fixed[3]

    return np.random.randn(*fixed).astype(dtype)

def test_gpu_inference():
    print("=" * 50)
    print("🔍 TEST D'INFÉRENCE GPU (ONNX Runtime)")
    print("=" * 50)

    # 1) Providers disponibles
    providers = ort.get_available_providers()
    print(f"Providers disponibles: {providers}")

    # 2) Modèle
    model_path = Path("models/emotion_vit_small.onnx")
    if not model_path.exists():
        print(f"❌ Modèle non trouvé: {model_path}")
        return False

    print(f"\n📦 Chargement du modèle: {model_path.name}")

    # 3) Créer la session en demandant CUDA puis CPU (fallback)
    try:
        session = ort.InferenceSession(
            str(model_path),
            providers=["CUDAExecutionProvider", "CPUExecutionProvider"],
        )
    except Exception as e:
        print(f"❌ Impossible de créer la session ORT: {e}")
        return False

    used_provider = session.get_providers()[0]
    if used_provider != "CUDAExecutionProvider":
        print("⚠️ CUDA demandé, mais ORT a basculé sur CPU.")
        print(f"✅ Provider utilisé: {used_provider}")
        print("   (Pour activer CUDA, il faut CUDA 12.x + cuDNN 9.x + runtime MSVC)")
    else:
        print(f"✅ Provider utilisé: {used_provider}")

    # 4) Infos inputs/outputs
    inputs = session.get_inputs()
    outputs_info = session.get_outputs()

    print("\n🧾 Inputs du modèle:")
    for i, inp in enumerate(inputs):
        print(f"   Input {i}: {inp.name}, shape: {inp.shape}, type: {inp.type}")

    print("\n🧾 Outputs du modèle:")
    for i, out in enumerate(outputs_info):
        print(f"   Output {i}: {out.name}, shape: {out.shape}, type: {out.type}")

    # 5) Créer un dummy input EXACTEMENT selon la shape attendue
    # Pour FER+ emotion-ferplus-8, c'est généralement [1,1,64,64] float
    input_name = inputs[0].name
    dummy_input = _make_dummy(inputs[0].shape, dtype=np.float32)

    # 6) Warm-up + bench
    print("\n🚀 Warm-up...")
    session.run(None, {input_name: dummy_input})

    runs = 50
    print(f"🏎️ Benchmark ({runs} runs)...")
    t0 = time.time()
    out0 = None
    for i in range(runs):
        out = session.run(None, {input_name: dummy_input})
        if i == 0:
            out0 = out
    t1 = time.time()

    avg_ms = (t1 - t0) * 1000 / runs

    # 7) Afficher les shapes
    if out0 is not None and len(out0) > 0:
        print(f"✅ Inférence OK. Output[0] shape: {np.array(out0[0]).shape}")
    print(f"⏱️ Temps moyen / inférence: {avg_ms:.2f} ms")

    # 8) Charger labels (avec nettoyage BOM au cas où)
    labels_path = Path("models/emotions.txt")
    if labels_path.exists():
        with open(labels_path, "r", encoding="utf-8-sig") as f:
            labels = [line.strip() for line in f if line.strip()]
        print(f"\n📋 Émotions ({len(labels)}): {', '.join(labels)}")

    return True

if __name__ == "__main__":
    ok = test_gpu_inference()
    raise SystemExit(0 if ok else 1)