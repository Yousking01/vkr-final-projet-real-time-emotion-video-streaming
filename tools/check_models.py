from __future__ import annotations

from pathlib import Path
import sys
import numpy as np

def main() -> int:
    print("=" * 60)
    print(" CHECK MODELS (ONNX + labels + GPU provider)")
    print("=" * 60)

    root = Path.cwd()
    model_path = root / "models" / "emotion_vit_small.onnx"
    labels_path = root / "models" / "emotions.txt"

    # --- basic file checks ---
    if not model_path.exists():
        print(f" Modèle introuvable: {model_path}")
        return 1
    if not labels_path.exists():
        print(f" Labels introuvables: {labels_path}")
        return 1

    size_mb = model_path.stat().st_size / (1024 * 1024)
    print(f" Modèle: {model_path} ({size_mb:.2f} MB)")
    print(f" Labels: {labels_path}")

    # --- read labels (handle BOM) ---
    labels = [l.strip() for l in labels_path.read_text(encoding="utf-8-sig").splitlines() if l.strip()]
    print(f" Nb labels: {len(labels)} -> {labels}")

    # --- onnx parse check ---
    try:
        import onnx
        m = onnx.load(str(model_path))
        print(f" ONNX load OK. Graph nodes: {len(m.graph.node)}")
    except Exception as e:
        print(f" ONNX load FAILED: {e}")
        return 1

    # --- onnxruntime check ---
    try:
        import onnxruntime as ort
    except Exception as e:
        print(f" onnxruntime import FAILED: {e}")
        return 1

    providers = ort.get_available_providers()
    print(f" ORT version: {ort.__version__}")
    print(f" Providers disponibles: {providers}")

    # Ask CUDA first, CPU fallback
    try:
        sess = ort.InferenceSession(
            str(model_path),
            providers=["CUDAExecutionProvider", "CPUExecutionProvider"],
        )
    except Exception as e:
        print(f" InferenceSession FAILED: {e}")
        return 1

    used = sess.get_providers()[0]
    print(f" Provider utilisé: {used}")
    if used != "CUDAExecutionProvider":
        print(" Attention: CUDA non utilisé (fallback CPU).")
        print("   Vérifie CUDA 12.x + cuDNN 9.x + MSVC runtime.")
    else:
        print(" GPU OK (CUDAExecutionProvider)")

    # --- input/output info ---
    inp = sess.get_inputs()[0]
    out = sess.get_outputs()[0]
    print("\n Input 0")
    print(f"  name : {inp.name}")
    print(f"  shape: {inp.shape}")
    print(f"  type : {inp.type}")

    print("\n Output 0")
    print(f"  name : {out.name}")
    print(f"  shape: {out.shape}")
    print(f"  type : {out.type}")

    # --- validate expected FER+ shapes ---
    expected_in = [1, 1, 64, 64]
    expected_out_last = 8

    in_shape = [int(x) if isinstance(x, int) else x for x in inp.shape]
    out_shape = [int(x) if isinstance(x, int) else x for x in out.shape]

    if in_shape != expected_in:
        print(f"\n Input shape inattendue. Attendu {expected_in}, obtenu {inp.shape}")
    else:
        print("\n Input shape = [1,1,64,64] (OK)")

    if len(out_shape) >= 2 and isinstance(out_shape[1], int) and out_shape[1] != expected_out_last:
        print(f" Output classes inattendues. Attendu 8, obtenu {out.shape}")
    else:
        print(" Output classes = 8 (OK)")

    if len(labels) != 8:
        print(f" Labels count != 8 (tu as {len(labels)}). Risque de décalage.")
    else:
        print(" Labels count = 8 (OK)")

    # --- run a tiny inference ---
    x = np.random.rand(1, 1, 64, 64).astype(np.float32)
    y = sess.run(None, {inp.name: x})[0]
    print(f"\n Inference OK. Output shape: {np.array(y).shape}")

    print("\n Tout est prêt.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())