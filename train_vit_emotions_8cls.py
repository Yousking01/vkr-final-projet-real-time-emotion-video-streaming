# import os
# import json
# import multiprocessing as mp
# from collections import Counter

# import torch
# import torch.nn as nn
# from torch.utils.data import DataLoader
# from torchvision import datasets, transforms
# from tqdm import tqdm
# import timm
# from sklearn.metrics import classification_report, accuracy_score


# def main():
#     # ========= CONFIG =========
#     DATA_DIR = r"C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\dataset_projet_final\FER2013"  # <-- CHANGE ICI si besoin
#     TRAIN_DIR = os.path.join(DATA_DIR, "train")
#     VAL_DIR   = os.path.join(DATA_DIR, "val")

#     OUT_DIR = "artifacts_vit_emotions_8cls"
#     os.makedirs(OUT_DIR, exist_ok=True)

#     MODEL_NAME = "vit_small_patch16_224"   # bon compromis vitesse/qualité
#     NUM_CLASSES = 8
#     EPOCHS = 12
#     BATCH_SIZE = 64
#     LR = 3e-4
#     WEIGHT_DECAY = 0.05
#     NUM_WORKERS = 2  # <-- tu peux garder >0 maintenant (Windows ok avec __main__)

#     DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
#     print("DEVICE:", DEVICE)

#     # ========= TRANSFORMS =========
#     # Tes images sont 48x48 grayscale, on convertit en 3 canaux et on resize pour ViT.
#     train_tf = transforms.Compose([
#         transforms.Grayscale(num_output_channels=3),
#         transforms.Resize((224, 224)),
#         transforms.RandomHorizontalFlip(p=0.5),
#         transforms.RandomRotation(10),
#         transforms.RandomApply([transforms.ColorJitter(brightness=0.2, contrast=0.2)], p=0.5),
#         transforms.ToTensor(),
#         transforms.Normalize(mean=(0.5, 0.5, 0.5), std=(0.5, 0.5, 0.5)),
#     ])

#     val_tf = transforms.Compose([
#         transforms.Grayscale(num_output_channels=3),
#         transforms.Resize((224, 224)),
#         transforms.ToTensor(),
#         transforms.Normalize(mean=(0.5, 0.5, 0.5), std=(0.5, 0.5, 0.5)),
#     ])

#     # ========= DATA =========
#     train_ds = datasets.ImageFolder(TRAIN_DIR, transform=train_tf)
#     val_ds   = datasets.ImageFolder(VAL_DIR, transform=val_tf)

#     print("Train size:", len(train_ds))
#     print("Val size:", len(val_ds))

#     # IMPORTANT: l'ordre des classes vient des noms de dossiers triés alphabétiquement
#     class_names = train_ds.classes
#     class_to_idx = train_ds.class_to_idx
#     print("Classes (order):", class_names)
#     print("class_to_idx:", class_to_idx)

#     # Sauvegarder l'ordre des classes (sera utilisé pour emotions.txt + dashboard)
#     with open(os.path.join(OUT_DIR, "class_names.json"), "w", encoding="utf-8") as f:
#         json.dump(class_names, f, ensure_ascii=False, indent=2)

#     with open(os.path.join(OUT_DIR, "emotions.txt"), "w", encoding="utf-8") as f:
#         f.write("\n".join(class_names))

#     # DataLoaders
#     pin_memory = (DEVICE == "cuda")
#     train_loader = DataLoader(
#         train_ds,
#         batch_size=BATCH_SIZE,
#         shuffle=True,
#         num_workers=NUM_WORKERS,
#         pin_memory=pin_memory,
#         persistent_workers=(NUM_WORKERS > 0),
#     )
#     val_loader = DataLoader(
#         val_ds,
#         batch_size=BATCH_SIZE,
#         shuffle=False,
#         num_workers=NUM_WORKERS,
#         pin_memory=pin_memory,
#         persistent_workers=(NUM_WORKERS > 0),
#     )

#     # ========= CLASS WEIGHTS (utile si léger déséquilibre) =========
#     train_labels = [y for _, y in train_ds.samples]
#     counts = Counter(train_labels)
#     weights = torch.tensor([1.0 / counts[i] for i in range(NUM_CLASSES)], dtype=torch.float32).to(DEVICE)
#     criterion = nn.CrossEntropyLoss(weight=weights)

#     # ========= MODEL =========
#     model = timm.create_model(MODEL_NAME, pretrained=True, num_classes=NUM_CLASSES)
#     model.to(DEVICE)

#     optimizer = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)

#     # Scheduler simple
#     scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=EPOCHS)

#     best_acc = 0.0
#     best_path = os.path.join(OUT_DIR, "best_vit_8cls.pth")

#     # ========= TRAIN / EVAL =========
#     for epoch in range(1, EPOCHS + 1):
#         model.train()
#         train_loss = 0.0

#         pbar = tqdm(train_loader, desc=f"Epoch {epoch}/{EPOCHS} [train]")
#         for x, y in pbar:
#             x, y = x.to(DEVICE, non_blocking=True), y.to(DEVICE, non_blocking=True)
#             optimizer.zero_grad(set_to_none=True)

#             logits = model(x)
#             loss = criterion(logits, y)
#             loss.backward()
#             optimizer.step()

#             train_loss += loss.item()
#             pbar.set_postfix(loss=train_loss / max(1, len(pbar)))

#         scheduler.step()

#         # ---- validation
#         model.eval()
#         all_preds, all_true = [], []
#         with torch.no_grad():
#             for x, y in tqdm(val_loader, desc=f"Epoch {epoch}/{EPOCHS} [val]"):
#                 x = x.to(DEVICE, non_blocking=True)
#                 logits = model(x)
#                 preds = torch.argmax(logits, dim=1).cpu().tolist()
#                 all_preds.extend(preds)
#                 all_true.extend(y.tolist())

#         acc = accuracy_score(all_true, all_preds)
#         print(f"\nEpoch {epoch}: train_loss={train_loss/len(train_loader):.4f}  val_acc={acc:.4f}")

#         if acc > best_acc:
#             best_acc = acc
#             torch.save(model.state_dict(), best_path)
#             print(f"✅ New best saved: {best_path} (acc={best_acc:.4f})")

#     print("\n==== FINAL REPORT (val) ====")
#     print("Best acc:", best_acc)
#     print(classification_report(all_true, all_preds, target_names=class_names))
#     print("\nSaved emotions.txt and class_names.json in:", OUT_DIR)


# if __name__ == "__main__":
#     mp.freeze_support()
#     main()

import os
import json
import multiprocessing as mp
from collections import Counter

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from tqdm import tqdm
import timm
from sklearn.metrics import classification_report, accuracy_score


def main():
    # ========= CONFIG =========
    DATA_DIR = r"C:\Users\Youssouf DJIRE\Desktop\SEMESTRE_3\Нирс\dataset_projet_final\FER2013"  # <-- CHANGE ICI si besoin
    TRAIN_DIR = os.path.join(DATA_DIR, "train")
    VAL_DIR = os.path.join(DATA_DIR, "val")

    OUT_DIR = "artifacts_vit_emotions_8cls"
    os.makedirs(OUT_DIR, exist_ok=True)

    MODEL_NAME = "vit_small_patch16_224"
    NUM_CLASSES = 8
    EPOCHS = 12
    BATCH_SIZE = 64
    LR = 3e-4
    WEIGHT_DECAY = 0.05

    # --- Reprise (resume) ---
    RESUME = True
    START_EPOCH = 3  # tu as déjà fini epoch 1 et 2
    best_path = os.path.join(OUT_DIR, "best_vit_8cls.pth")

    # --- DataLoader stabilité Windows ---
    # Si tu veux le max de stabilité: NUM_WORKERS=0
    NUM_WORKERS = 0  # <-- recommandé sur Windows (évite les crashs sporadiques)
    USE_AMP = True   # accélère + réduit VRAM sur GPU

    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    print("DEVICE:", DEVICE)

    # ========= TRANSFORMS =========
    train_tf = transforms.Compose([
        transforms.Grayscale(num_output_channels=3),
        transforms.Resize((224, 224)),
        transforms.RandomHorizontalFlip(p=0.5),
        transforms.RandomRotation(10),
        transforms.RandomApply([transforms.ColorJitter(brightness=0.2, contrast=0.2)], p=0.5),
        transforms.ToTensor(),
        transforms.Normalize(mean=(0.5, 0.5, 0.5), std=(0.5, 0.5, 0.5)),
    ])

    val_tf = transforms.Compose([
        transforms.Grayscale(num_output_channels=3),
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(mean=(0.5, 0.5, 0.5), std=(0.5, 0.5, 0.5)),
    ])

    # ========= DATA =========
    train_ds = datasets.ImageFolder(TRAIN_DIR, transform=train_tf)
    val_ds = datasets.ImageFolder(VAL_DIR, transform=val_tf)

    print("Train size:", len(train_ds))
    print("Val size:", len(val_ds))

    class_names = train_ds.classes
    class_to_idx = train_ds.class_to_idx
    print("Classes (order):", class_names)
    print("class_to_idx:", class_to_idx)

    # Sauvegarder l'ordre des classes
    with open(os.path.join(OUT_DIR, "class_names.json"), "w", encoding="utf-8") as f:
        json.dump(class_names, f, ensure_ascii=False, indent=2)

    with open(os.path.join(OUT_DIR, "emotions.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(class_names))

    # DataLoaders (stables)
    pin_memory = (DEVICE == "cuda")
    train_loader = DataLoader(
        train_ds,
        batch_size=BATCH_SIZE,
        shuffle=True,
        num_workers=NUM_WORKERS,
        pin_memory=pin_memory,
        persistent_workers=False,  # important pour stabilité Windows
    )
    val_loader = DataLoader(
        val_ds,
        batch_size=BATCH_SIZE,
        shuffle=False,
        num_workers=NUM_WORKERS,
        pin_memory=pin_memory,
        persistent_workers=False,
    )

    # ========= CLASS WEIGHTS =========
    train_labels = [y for _, y in train_ds.samples]
    counts = Counter(train_labels)
    weights = torch.tensor([1.0 / counts[i] for i in range(NUM_CLASSES)], dtype=torch.float32).to(DEVICE)
    criterion = nn.CrossEntropyLoss(weight=weights)

    # ========= MODEL =========
    model = timm.create_model(MODEL_NAME, pretrained=True, num_classes=NUM_CLASSES)
    model.to(DEVICE)

    optimizer = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=EPOCHS)

    # Resume depuis best
    best_acc = 0.0
    if RESUME and os.path.exists(best_path):
        print("✅ Resume: loading", best_path)
        model.load_state_dict(torch.load(best_path, map_location="cpu"))
        best_acc = 0.0  # on laisse le script recalculer les meilleurs epochs à partir de maintenant

    # AMP scaler
    scaler = torch.cuda.amp.GradScaler(enabled=(USE_AMP and DEVICE == "cuda"))

    # ========= TRAIN / EVAL =========
    for epoch in range(START_EPOCH if RESUME else 1, EPOCHS + 1):
        model.train()
        train_loss = 0.0

        pbar = tqdm(train_loader, desc=f"Epoch {epoch}/{EPOCHS} [train]")
        for x, y in pbar:
            x = x.to(DEVICE, non_blocking=True)
            y = y.to(DEVICE, non_blocking=True)
            optimizer.zero_grad(set_to_none=True)

            with torch.cuda.amp.autocast(enabled=(USE_AMP and DEVICE == "cuda")):
                logits = model(x)
                loss = criterion(logits, y)

            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()

            train_loss += loss.item()
            pbar.set_postfix(loss=train_loss / max(1, len(pbar)))

        scheduler.step()

        # ---- validation
        model.eval()
        all_preds, all_true = [], []
        with torch.no_grad():
            for x, y in tqdm(val_loader, desc=f"Epoch {epoch}/{EPOCHS} [val]"):
                x = x.to(DEVICE, non_blocking=True)
                with torch.cuda.amp.autocast(enabled=(USE_AMP and DEVICE == "cuda")):
                    logits = model(x)
                preds = torch.argmax(logits, dim=1).cpu().tolist()
                all_preds.extend(preds)
                all_true.extend(y.tolist())

        acc = accuracy_score(all_true, all_preds)
        print(f"\nEpoch {epoch}: train_loss={train_loss/len(train_loader):.4f}  val_acc={acc:.4f}")

        # Sauvegarde meilleur modèle
        if acc > best_acc:
            best_acc = acc
            torch.save(model.state_dict(), best_path)
            print(f"✅ New best saved: {best_path} (acc={best_acc:.4f})")

    print("\n==== FINAL REPORT (val) ====")
    print("Best acc:", best_acc)
    print(classification_report(all_true, all_preds, target_names=class_names))
    print("\nSaved emotions.txt and class_names.json in:", OUT_DIR)


if __name__ == "__main__":
    mp.freeze_support()
    main()