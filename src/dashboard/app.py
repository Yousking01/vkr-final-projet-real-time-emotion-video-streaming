# import os
# import time
# import json
# import threading
# from typing import Optional, Tuple, List, Dict

# import numpy as np
# import pandas as pd
# import streamlit as st
# import plotly.express as px

# # --- imports live ---

# import mediapipe as mp
# import onnxruntime as ort
# from collections import deque

# from dotenv import load_dotenv
# from streamlit_autorefresh import st_autorefresh

# from pathlib import Path

# APP_DIR = Path(__file__).resolve().parent        # .../src/dashboard
# PROJECT_ROOT = APP_DIR.parents[2]                # .../Final_project

# import cv2
# import av
# from streamlit_webrtc import (
#     webrtc_streamer,
#     VideoProcessorBase,
#     WebRtcMode,
#     RTCConfiguration,
# )

# # DuckDB (optional but recommended)
# try:
#     import duckdb  # pip install duckdb
#     DUCKDB_AVAILABLE = True
# except Exception:
#     duckdb = None  # type: ignore
#     DUCKDB_AVAILABLE = False

# # Kafka (OPTIONAL)
# try:
#     from kafka import KafkaProducer  # pip install kafka-python
#     KAFKA_AVAILABLE = True
# except Exception:
#     KafkaProducer = None  # type: ignore
#     KAFKA_AVAILABLE = False

# load_dotenv()

# # ============================
# # Thread-safe LIVE config
# # ============================
# # LIVE_CFG_LOCK = threading.Lock()
# # LIVE_CFG: Dict[str, object] = {
# #     "source_id": "cam_01",
# #     "send_kafka": bool(KAFKA_AVAILABLE),
# #     "send_interval": 0.5,
# #     "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP_WIN", "localhost:9092"),
# #     "kafka_topic": os.getenv("KAFKA_TOPIC", "test_topic"),
# #     "detect_every_n": 3,
# #     "hold_boxes_ms": 450,
# # }

# # --- config live partagé (déjà ton approche) ---
# LIVE_CFG_LOCK = threading.Lock()
# LIVE_CFG = {
#     "source_id": "cam_01",
#     "send_kafka": False,
#     "send_interval": 0.5,
#     "kafka_bootstrap": "localhost:9092",  # Windows -> 9092
#     "kafka_topic": "test_topic",
#     "detect_every_n": 3,
#     "hold_boxes_ms": 450,
#     "ema_alpha": 0.6,              # smoothing EMA
#     "vote_window": 15,             # ~0.5s si ~30fps
#     "model_path": "models/emotion_vit_small_fp16.onnx",
#     "labels_path": "models/emotions.txt",
# }

# def softmax(x: np.ndarray) -> np.ndarray:
#     x = x - np.max(x)
#     e = np.exp(x)
#     return e / np.sum(e)

# class EmotionViTOnnx:
#     def __init__(self, model_path: str, labels_path: str):
#         self.labels = [l.strip() for l in open(labels_path, "r", encoding="utf-8") if l.strip()]
#         self.sess = self._make_session(model_path)
#         self.input_name = self.sess.get_inputs()[0].name

#         # assume output logits [1, num_classes]
#         self.output_name = self.sess.get_outputs()[0].name

#     def _make_session(self, model_path: str) -> ort.InferenceSession:
#         providers = ort.get_available_providers()
#         # Prefer CUDA -> else DirectML -> else CPU
#         if "CUDAExecutionProvider" in providers:
#             prov = ["CUDAExecutionProvider", "CPUExecutionProvider"]
#         elif "DmlExecutionProvider" in providers:
#             prov = ["DmlExecutionProvider", "CPUExecutionProvider"]
#         else:
#             prov = ["CPUExecutionProvider"]

#         so = ort.SessionOptions()
#         so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
#         return ort.InferenceSession(model_path, sess_options=so, providers=prov)

#     def preprocess(self, face_bgr: np.ndarray) -> np.ndarray:
#         # ViT généralement 224x224
#         img = cv2.resize(face_bgr, (224, 224), interpolation=cv2.INTER_AREA)
#         img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB).astype(np.float32) / 255.0

#         # Normalisation ImageNet (souvent utilisée). Adapte si ton modèle attend autre chose.
#         mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
#         std  = np.array([0.229, 0.224, 0.225], dtype=np.float32)
#         img = (img - mean) / std

#         # NCHW
#         img = np.transpose(img, (2, 0, 1))[None, ...]
#         return img

#     def predict(self, face_bgr: np.ndarray) -> tuple[str, float, np.ndarray]:
#         x = self.preprocess(face_bgr)
#         logits = self.sess.run([self.output_name], {self.input_name: x})[0][0]
#         probs = softmax(logits.astype(np.float32))
#         idx = int(np.argmax(probs))
#         label = self.labels[idx] if idx < len(self.labels) else f"class_{idx}"
#         return label, float(probs[idx]), probs

# class FaceKafkaWebRTCProcessor(VideoProcessorBase):
#     def __init__(self):
#         self.mp_face = mp.solutions.face_detection.FaceDetection(
#             model_selection=0, min_detection_confidence=0.5
#         )

#         self.frame_id = 0
#         self.last_sent_ts = 0.0
#         self.last_flush_ts = 0.0

#         self._producer = None
#         self._producer_bootstrap = ""
#         self._producer_topic = ""

#         self._last_boxes = []
#         self._last_boxes_ts = 0.0

#         self.model = None
#         self.model_path = ""
#         self.labels_path = ""

#         # smoothing
#         self.ema = None
#         self.alpha = 0.6
#         self.vote = deque(maxlen=15)

#     def _ensure_producer(self, enabled: bool, bootstrap: str, topic: str):
#         if not enabled or not KAFKA_AVAILABLE:
#             if self._producer is not None:
#                 try: self._producer.flush(timeout=0.2)
#                 except: pass
#                 try: self._producer.close(timeout=1)
#                 except: pass
#             self._producer = None
#             self._producer_bootstrap = ""
#             self._producer_topic = ""
#             return

#         if (self._producer is None) or (self._producer_bootstrap != bootstrap) or (self._producer_topic != topic):
#             self._producer = make_kafka_producer(bootstrap)
#             self._producer_bootstrap = bootstrap
#             self._producer_topic = topic

#     def _ensure_model(self, model_path: str, labels_path: str):
#         if self.model is None or self.model_path != model_path or self.labels_path != labels_path:
#             self.model = EmotionViTOnnx(model_path, labels_path)
#             self.model_path = model_path
#             self.labels_path = labels_path
#             self.ema = None
#             self.vote.clear()

#     def _mp_detect(self, img_bgr: np.ndarray):
#         img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
#         res = self.mp_face.process(img_rgb)
#         boxes = []
#         if res.detections:
#             h, w = img_bgr.shape[:2]
#             for det in res.detections:
#                 b = det.location_data.relative_bounding_box
#                 x0 = int(b.xmin * w)
#                 y0 = int(b.ymin * h)
#                 bw = int(b.width * w)
#                 bh = int(b.height * h)
#                 boxes.append((x0, y0, bw, bh, float(det.score[0] if det.score else 0.8)))
#         return boxes

#     def recv(self, frame: av.VideoFrame) -> av.VideoFrame:
#         img_bgr = frame.to_ndarray(format="bgr24")
#         H, W = img_bgr.shape[:2]
#         self.frame_id += 1
#         now = time.time()

#         with LIVE_CFG_LOCK:
#             cfg = dict(LIVE_CFG)

#         source_id = str(cfg["source_id"])
#         send_kafka = bool(cfg["send_kafka"])
#         send_interval = float(cfg["send_interval"])
#         kafka_bootstrap = str(cfg["kafka_bootstrap"])
#         kafka_topic = str(cfg["kafka_topic"])
#         detect_every_n = int(cfg["detect_every_n"])
#         hold_boxes_ms = int(cfg["hold_boxes_ms"])
#         self.alpha = float(cfg.get("ema_alpha", 0.6))
#         vote_window = int(cfg.get("vote_window", 15))
#         self.vote = deque(self.vote, maxlen=max(3, vote_window))

#         model_path = str(cfg["model_path"])
#         labels_path = str(cfg["labels_path"])

#         self._ensure_model(model_path, labels_path)
#         self._ensure_producer(send_kafka, kafka_bootstrap, kafka_topic)

#         # detection 1/N + hold
#         if (self.frame_id % max(1, detect_every_n)) == 0:
#             boxes = self._mp_detect(img_bgr)
#             self._last_boxes = boxes
#             self._last_boxes_ts = now
#         else:
#             age_ms = (now - self._last_boxes_ts) * 1000.0
#             boxes = self._last_boxes if (self._last_boxes and age_ms <= max(0, hold_boxes_ms)) else []

#         # (simple) take best box
#         if boxes:
#             boxes = sorted(boxes, key=lambda b: b[4], reverse=True)
#             x, y, bw, bh, conf = boxes[0]
#             x0 = max(0, x); y0 = max(0, y)
#             x1 = min(W, x0 + bw); y1 = min(H, y0 + bh)

#             if x1 > x0 and y1 > y0:
#                 face = img_bgr[y0:y1, x0:x1]

#                 label, score, probs = self.model.predict(face)

#                 # EMA smoothing
#                 if self.ema is None:
#                     self.ema = probs
#                 else:
#                     self.ema = (self.alpha * probs) + ((1.0 - self.alpha) * self.ema)

#                 label_ema = self.model.labels[int(np.argmax(self.ema))]
#                 score_ema = float(np.max(self.ema))

#                 # vote smoothing
#                 self.vote.append(label_ema)
#                 label_vote = max(set(self.vote), key=self.vote.count)

#                 # draw
#                 cv2.rectangle(img_bgr, (x0, y0), (x1, y1), (0,255,0), 2)
#                 cv2.putText(
#                     img_bgr,
#                     f"{label_vote} ({score_ema:.2f})",
#                     (x0, max(24, y0-10)),
#                     cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0,255,0), 2
#                 )

#                 # Kafka throttle (identique à ton workflow)
#                 if self._producer is not None and (now - self.last_sent_ts) >= send_interval:
#                     self.last_sent_ts = now
#                     event = {
#                         "source_id": source_id,
#                         "frame_id": int(self.frame_id),
#                         "face_id": 1,
#                         "producer_ts": float(now),
#                         "emotion": str(label_vote),
#                         "score": float(score_ema),
#                         "bbox_x": int(x0),
#                         "bbox_y": int(y0),
#                         "bbox_w": int(x1-x0),
#                         "bbox_h": int(y1-y0),
#                     }
#                     try:
#                         self._producer.send(kafka_topic, event)
#                     except Exception:
#                         pass

#                     if (now - self.last_flush_ts) > 1.2:
#                         self.last_flush_ts = now
#                         try:
#                             self._producer.flush(timeout=0.2)
#                         except Exception:
#                             pass

#         return av.VideoFrame.from_ndarray(img_bgr, format="bgr24")

# # ---------------------------
# # Small helper: rerun
# # ---------------------------
# def do_rerun():
#     try:
#         st.rerun()
#     except Exception:
#         try:
#             st.experimental_rerun()
#         except Exception:
#             pass

# # ---------------------------
# # I18N
# # ---------------------------
# I18N = {
#     "en": {
#         "sidebar_title": "Controls",
#         "tab_analytics": "Analytics (Parquet)",
#         "tab_live": "Live Webcam (WebRTC - Fast)",
#         "data_path": "Parquet folder",
#         "refresh": "Refresh",
#         "auto_refresh": "Auto refresh (seconds)",
#         "source_filter": "Source (camera)",
#         "time_filter": "Time filter (last N minutes)",
#         "min_score": "Min score",
#         "lat_p50": "E2E Latency p50",
#         "lat_p95": "E2E Latency p95",
#         "proc_p50": "Processing p50",
#         "proc_p95": "Processing p95",
#         "events_rate": "Events (rows)",
#         "latest_events": "Latest Events",
#         "distribution": "Emotion Distribution",
#         "timeline": "Detections Over Time",
#         "no_data": "No data found in the selected folder.",
#         "note": "Tip: keep Spark job running to see live updates.",
#         "no_ts_found": "No usable timestamp column found. Showing rows without time filter/sorting.",
#         "no_emotion": "No emotion column found.",
#         "timeline_needs": "Timeline needs a timestamp column + emotion column.",
#         "no_events_in_window": "No events in selected time window. Send messages to Kafka or increase the time window.",
#         "last_updated": "Last updated",
#         "live_help": "WebRTC webcam (fast) + Haar faces + emotion stub + optional Kafka throttle",
#         "live_source_id": "source_id",
#         "live_send_kafka": "Send events to Kafka",
#         "live_send_interval": "Min seconds between sends",
#         "live_kafka_bootstrap": "Kafka bootstrap (Windows)",
#         "live_topic": "Kafka topic",
#         "live_width": "Camera width",
#         "live_height": "Camera height",
#         "live_detect_every": "Detect faces every N frames",
#         "live_hold_ms": "Hold boxes (ms)",
#         "live_start": "Start Live",
#         "live_stop": "Stop Live",
#         "live_status": "Status",
#         "expected": "✅ Expected: smooth video. If Kafka ON, Spark writes Parquet and it appears in Analytics.",
#         "kafka_missing": "Kafka is not available (install: pip install kafka-python). Live can run without Kafka.",
#     },
#     "ru": {
#         "sidebar_title": "Управление",
#         "tab_analytics": "Аналитика (Parquet)",
#         "tab_live": "Веб-камера (WebRTC - быстро)",
#         "data_path": "Папка Parquet",
#         "refresh": "Обновить",
#         "auto_refresh": "Автообновление (сек.)",
#         "source_filter": "Источник (камера)",
#         "time_filter": "Фильтр времени (последние N минут)",
#         "min_score": "Мин. уверенность (score)",
#         "lat_p50": "E2E задержка p50",
#         "lat_p95": "E2E задержка p95",
#         "proc_p50": "Обработка p50",
#         "proc_p95": "Обработка p95",
#         "events_rate": "События (строки)",
#         "latest_events": "Последние события",
#         "distribution": "Распределение эмоций",
#         "timeline": "Детекции во времени",
#         "no_data": "В выбранной папке нет данных.",
#         "note": "Совет: оставь Spark job запущенным, чтобы видеть обновления.",
#         "no_ts_found": "Не найдена подходящая колонка времени. Показываю без фильтра/сортировки по времени.",
#         "no_emotion": "Колонка emotion не найдена.",
#         "timeline_needs": "Для таймлайна нужны колонка времени и колонка emotion.",
#         "no_events_in_window": "В выбранном окне времени нет событий. Отправь сообщения в Kafka или увеличь окно.",
#         "last_updated": "Последнее обновление",
#         "live_help": "WebRTC веб-камера (быстро) + Haar лица + эмоции + Kafka throttle",
#         "live_source_id": "source_id",
#         "live_send_kafka": "Отправлять события в Kafka",
#         "live_send_interval": "Мин. секунды между отправками",
#         "live_kafka_bootstrap": "Kafka bootstrap (Windows)",
#         "live_topic": "Kafka topic",
#         "live_width": "Ширина камеры",
#         "live_height": "Высота камеры",
#         "live_detect_every": "Детект каждые N кадров",
#         "live_hold_ms": "Держать боксы (мс)",
#         "live_start": "Старт Live",
#         "live_stop": "Стоп Live",
#         "live_status": "Статус",
#         "expected": "✅ Ожидаемо: плавное видео. Если Kafka ON, Spark пишет Parquet и это видно в Analytics.",
#         "kafka_missing": "Kafka недоступен (установи: pip install kafka-python). Live работает и без Kafka.",
#     },
# }

# # ---------------------------
# # CSS minimal (tu peux remettre ton CSS premium)
# # ---------------------------
# APP_CSS = r"""
# <style>
# .block-container { max-width: none !important; padding-top: 0.70rem; padding-left: 2.2rem; padding-right: 2.2rem; }
# #MainMenu {visibility: hidden;} footer {visibility: hidden;}
# </style>
# """

# # ---------------------------
# # Emotion stub
# # ---------------------------
# EMO_LIST = ["happy", "sad", "angry", "neutral", "surprised", "fear"]

# EMO_COLOR_MAP: Dict[str, str] = {
#     "happy": "#1f77b4",
#     "sad": "#d62728",
#     "angry": "#ff7f0e",
#     "neutral": "#7f7f7f",
#     "surprised": "#9467bd",
#     "fear": "#bcbd22",
# }

# def infer_emotion_stub(face_rgb: np.ndarray) -> Tuple[str, float]:
#     label = EMO_LIST[int(time.time()) % len(EMO_LIST)]
#     score = 0.70 + 0.20 * ((int(time.time() * 10) % 5) / 5)
#     return label, float(min(score, 0.95))

# # ---------------------------
# # Haar face detector
# # ---------------------------
# @st.cache_resource
# def get_haar_face_detector():
#     cascade_path = os.path.join(cv2.data.haarcascades, "haarcascade_frontalface_default.xml")
#     cascade = cv2.CascadeClassifier(cascade_path)
#     if cascade.empty():
#         raise RuntimeError(f"Failed to load Haar cascade: {cascade_path}")
#     return cascade

# def detect_faces_haar_fast(cascade, frame_rgb: np.ndarray) -> List[Tuple[int, int, int, int, float]]:
#     gray = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2GRAY)
#     h, w = gray.shape[:2]
#     target_w = 640
#     scale = 1.0
#     if w > target_w:
#         scale = target_w / float(w)
#         new_h = max(1, int(h * scale))
#         gray_small = cv2.resize(gray, (target_w, new_h), interpolation=cv2.INTER_AREA)
#     else:
#         gray_small = gray

#     faces = cascade.detectMultiScale(
#         gray_small,
#         scaleFactor=1.1,
#         minNeighbors=5,
#         minSize=(40, 40),
#     )

#     boxes: List[Tuple[int, int, int, int, float]] = []
#     inv = 1.0 / scale
#     for (x, y, fw, fh) in faces:
#         x0 = int(x * inv)
#         y0 = int(y * inv)
#         w0 = int(fw * inv)
#         h0 = int(fh * inv)
#         boxes.append((x0, y0, w0, h0, 0.80))
#     return boxes

# # ---------------------------
# # Kafka helpers
# # ---------------------------
# def make_kafka_producer(bootstrap: str):
#     if not KAFKA_AVAILABLE or not bootstrap:
#         return None
#     return KafkaProducer(
#         bootstrap_servers=bootstrap,
#         value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
#         retries=2,
#         acks=1,
#         linger_ms=10,
#     )

# # ---------------------------
# # Parquet loading (DuckDB accelerated)
# # ---------------------------
# def _normalize_path_for_duckdb(path: str) -> str:
#     return path.replace("\\", "/")

# # @st.cache_data(ttl=2)
# # def load_parquet_folder(path: str) -> pd.DataFrame:
# #     if not path or not os.path.exists(path):
# #         return pd.DataFrame()

# #     if DUCKDB_AVAILABLE:
# #         try:
# #             p = _normalize_path_for_duckdb(path)
# #             con = duckdb.connect(database=":memory:", read_only=False)
# #             df = con.execute(
# #                 f"SELECT * FROM read_parquet('{p}', hive_partitioning=true, union_by_name=true)"
# #             ).df()
# #             con.close()
# #             return df
# #         except Exception:
# #             pass

# #     # fallback
# #     try:
# #         import pyarrow.dataset as ds
# #         dataset = ds.dataset(path, format="parquet", partitioning="hive")
# #         table = dataset.to_table()
# #         return table.to_pandas()
# #     except Exception:
# #         try:
# #             return pd.read_parquet(path, engine="pyarrow")
# #         except Exception:
# #             return pd.DataFrame()

# @st.cache_data(ttl=2)
# def load_parquet_folder(path: str) -> pd.DataFrame:
#     if not path:
#         return pd.DataFrame()

#     p = Path(path)

#     # si relatif + introuvable -> on le recale sur la racine du projet
#     if not p.is_absolute() and not p.exists():
#         p = (PROJECT_ROOT / p).resolve()

#     if not p.exists():
#         return pd.DataFrame()

#     # DuckDB (si dispo)
#     if DUCKDB_AVAILABLE:
#         try:
#             # pp = str(p).replace("\\", "/")
#             # globp = pp.rstrip("/") + "/**/*.parquet"
#             # con = duckdb.connect(database=":memory:")
#             # df = con.execute(
#             #     f"SELECT * FROM read_parquet('{globp}', hive_partitioning=true, union_by_name=true)"
#             # ).df()
#             pp = str(p).replace("\\", "/")
#             base = pp.rstrip("/")
#             glob1 = base + "/*.parquet"
#             glob2 = base + "/*/*.parquet"
#             glob3 = base + "/*/*/*.parquet"

#             con = duckdb.connect(database=":memory:")
#             df = con.execute(
#                 f"""
#                 SELECT * FROM read_parquet(
#                 ['{glob1}','{glob2}','{glob3}'],
#                 hive_partitioning=true,
#                 union_by_name=true
#                 )
#                 """
#             ).df()
#             con.close()
#             return df
#             # con.close()
#             # return df
#         except Exception:
#             pass

#     # fallback pyarrow
#     try:
#         import pyarrow.dataset as ds
#         dataset = ds.dataset(str(p), format="parquet", partitioning="hive")
#         return dataset.to_table().to_pandas()
#     except Exception:
#         try:
#             return pd.read_parquet(str(p), engine="pyarrow")
#         except Exception:
#             return pd.DataFrame()

# def percentile_safe(series: pd.Series, q: float) -> float:
#     s = pd.to_numeric(series, errors="coerce").dropna()
#     if len(s) == 0:
#         return np.nan
#     return float(np.percentile(s, q))

# def pick_timestamp_column(df: pd.DataFrame) -> Optional[str]:
#     candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time"]
#     for c in candidates:
#         if c in df.columns:
#             df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
#             if not df[c].isna().all():
#                 return c
#     return None

# # ============================
# # WebRTC Processor
# # ============================
# class FaceKafkaWebRTCProcessor(VideoProcessorBase):
#     def __init__(self):
#         self.cascade = get_haar_face_detector()
#         self.frame_id = 0
#         self.last_sent_ts = 0.0
#         self.last_flush_ts = 0.0

#         self._producer = None
#         self._producer_bootstrap = ""
#         self._producer_topic = ""

#         self._last_boxes: List[Tuple[int, int, int, int, float]] = []
#         self._last_boxes_ts = 0.0

#     def _ensure_producer(self, enabled: bool, bootstrap: str, topic: str):
#         if not enabled or not KAFKA_AVAILABLE:
#             if self._producer is not None:
#                 try:
#                     self._producer.flush(timeout=0.2)
#                 except Exception:
#                     pass
#                 try:
#                     self._producer.close(timeout=1)
#                 except Exception:
#                     pass
#             self._producer = None
#             self._producer_bootstrap = ""
#             self._producer_topic = ""
#             return

#         if self._producer is None or self._producer_bootstrap != bootstrap or self._producer_topic != topic:
#             try:
#                 self._producer = make_kafka_producer(bootstrap)
#                 self._producer_bootstrap = bootstrap
#                 self._producer_topic = topic
#             except Exception:
#                 self._producer = None
#                 self._producer_bootstrap = ""
#                 self._producer_topic = ""

#     def recv(self, frame: av.VideoFrame) -> av.VideoFrame:
#         img_bgr = frame.to_ndarray(format="bgr24")

#         with LIVE_CFG_LOCK:
#             cfg = dict(LIVE_CFG)

#         source_id = str(cfg.get("source_id", "cam_01"))
#         send_kafka = bool(cfg.get("send_kafka", False))
#         send_interval = float(cfg.get("send_interval", 0.5))
#         kafka_bootstrap = str(cfg.get("kafka_bootstrap", "localhost:9092"))
#         kafka_topic = str(cfg.get("kafka_topic", "test_topic"))
#         detect_every_n = int(cfg.get("detect_every_n", 3))
#         hold_boxes_ms = int(cfg.get("hold_boxes_ms", 450))

#         self._ensure_producer(send_kafka, kafka_bootstrap, kafka_topic)

#         self.frame_id += 1
#         frame_count = self.frame_id

#         img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)

#         if (frame_count % max(1, detect_every_n)) == 0:
#             boxes = detect_faces_haar_fast(self.cascade, img_rgb)
#             self._last_boxes = boxes
#             self._last_boxes_ts = time.time()
#         else:
#             age_ms = (time.time() - self._last_boxes_ts) * 1000.0
#             if self._last_boxes and age_ms <= max(0, hold_boxes_ms):
#                 boxes = self._last_boxes
#             else:
#                 boxes = []

#         H, W = img_rgb.shape[:2]
#         now = time.time()

#         face_id = 0
#         for (x, y, w, h, conf) in boxes:
#             face_id += 1
#             x0 = max(0, int(x))
#             y0 = max(0, int(y))
#             x1 = min(W, int(x + w))
#             y1 = min(H, int(y + h))
#             if x1 <= x0 or y1 <= y0:
#                 continue

#             face_rgb = img_rgb[y0:y1, x0:x1]
#             emo, score = infer_emotion_stub(face_rgb)

#             cv2.rectangle(img_bgr, (x0, y0), (x1, y1), (0, 255, 0), 2)
#             cv2.putText(
#                 img_bgr,
#                 f"{emo.capitalize()} ({score:.2f})",
#                 (x0, max(26, y0 - 10)),
#                 cv2.FONT_HERSHEY_SIMPLEX,
#                 0.65,
#                 (0, 255, 0),
#                 2,
#             )

#             if self._producer is not None and (now - self.last_sent_ts) >= send_interval:
#                 self.last_sent_ts = now
#                 event = {
#                     "source_id": source_id,
#                     "frame_id": int(frame_count),
#                     "face_id": int(face_id),
#                     "producer_ts": float(now),
#                     "emotion": str(emo),
#                     "score": float(score),
#                     "bbox_x": int(x0),
#                     "bbox_y": int(y0),
#                     "bbox_w": int(x1 - x0),
#                     "bbox_h": int(y1 - y0),
#                 }
#                 try:
#                     self._producer.send(kafka_topic, event)
#                 except Exception:
#                     pass

#                 if (now - float(self.last_flush_ts)) > 1.2:
#                     self.last_flush_ts = now
#                     try:
#                         self._producer.flush(timeout=0.2)
#                     except Exception:
#                         pass

#         return av.VideoFrame.from_ndarray(img_bgr, format="bgr24")

# # ============================
# # Streamlit UI
# # ============================
# st.set_page_config(page_title="Emotion Dashboard", layout="wide", initial_sidebar_state="expanded")
# st.markdown(APP_CSS, unsafe_allow_html=True)

# lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
# t = I18N[lang]
# st.sidebar.header(t["sidebar_title"])

# st.markdown(
#     """
#     <div style="padding:14px 16px; border-radius:14px; background:#0f1420; color:#fff; margin: 8px 0 12px 0;">
#       <div style="font-size:26px; font-weight:900;">Real-Time Emotion Analytics Dashboard</div>
#       <div style="opacity:0.78; font-weight:700; margin-top:6px;">
#         Spark → Parquet (Analytics) · Webcam → Kafka (Live)
#       </div>
#     </div>
#     """,
#     unsafe_allow_html=True,
# )

# tab_analytics, tab_live = st.tabs([t["tab_analytics"], t["tab_live"]])

# def render_topbar(title: str):
#     st.markdown(
#         f"""
#         <div style="padding:10px 14px; border-radius:12px; background:#243a66; color:white; margin-bottom:10px;">
#           <div style="font-size:22px; font-weight:900;">{title}</div>
#           <div style="font-size:12px; opacity:0.85; font-weight:700;">
#             Spark → Parquet (Analytics)  |  Webcam → Kafka (Live)
#           </div>
#         </div>
#         """,
#         unsafe_allow_html=True,
#     )

# # ============================================================
# # TAB 1: Analytics (NO FRAGMENTS)
# # ============================================================
# with tab_analytics:
#     render_topbar(t["tab_analytics"])

#     default_path = os.getenv("PARQUET_PATH", "data/parquet_events_v2")
#     parquet_path = st.sidebar.text_input(t["data_path"], value=default_path)

#     min_minutes = st.sidebar.number_input(t["time_filter"], min_value=1, max_value=1440, value=60, step=5)
#     min_score = st.sidebar.slider(t["min_score"], min_value=0.0, max_value=1.0, value=0.5, step=0.05)

#     refresh_now = st.sidebar.button(t["refresh"])
#     auto_refresh_s = st.sidebar.number_input(t["auto_refresh"], min_value=0, max_value=3600, value=10, step=1)
#     st.sidebar.caption(t["note"])

#     if refresh_now:
#         st.cache_data.clear()

#     if int(auto_refresh_s) > 0:
#         st_autorefresh(interval=int(auto_refresh_s) * 1000, key="autorefresh_analytics")

#     df = load_parquet_folder(parquet_path)
#     if df.empty:
#         st.warning(t["no_data"])
#     else:
#         # timestamps
#         if "event_time" in df.columns and "event_time_dt" not in df.columns:
#             df["event_time_dt"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)

#         for c in ["event_ts", "spark_ts", "event_time_dt"]:
#             if c in df.columns:
#                 df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

#         ts_base = pick_timestamp_column(df)
#         if ts_base is None:
#             st.info(t["no_ts_found"])

#         if ts_base is not None:
#             now_ts = df[ts_base].max()
#             if pd.notna(now_ts):
#                 cutoff = now_ts - pd.Timedelta(minutes=int(min_minutes))
#                 df = df[df[ts_base] >= cutoff]

#         if df.empty:
#             st.warning(t["no_events_in_window"])
#         else:
#             if "score" in df.columns:
#                 df = df[pd.to_numeric(df["score"], errors="coerce").fillna(0) >= float(min_score)]

#             source_col = "source_id" if "source_id" in df.columns else None
#             if source_col:
#                 sources = sorted(df[source_col].dropna().unique().tolist())
#                 selected = st.sidebar.selectbox(t["source_filter"], ["ALL"] + sources, index=0, key="src_filter")
#                 if selected != "ALL":
#                     df = df[df[source_col] == selected]

#             if df.empty:
#                 st.warning(t["no_events_in_window"])
#             else:
#                 lat_p50 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 50)
#                 lat_p95 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 95)
#                 proc_p50 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 50)
#                 proc_p95 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 95)

#                 def fmt(x: float) -> str:
#                     return "—" if np.isnan(x) else f"{x:.0f}"

#                 c1, c2, c3, c4, c5 = st.columns(5)
#                 c1.metric(t["lat_p50"], fmt(lat_p50))
#                 c2.metric(t["lat_p95"], fmt(lat_p95))
#                 c3.metric(t["proc_p50"], fmt(proc_p50))
#                 c4.metric(t["proc_p95"], fmt(proc_p95))
#                 c5.metric(t["events_rate"], f"{len(df)}")

#                 st.caption(f"{t['last_updated']}: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

#                 left, right = st.columns([1.35, 1.0], gap="large")

#                 with left:
#                     st.subheader(t["latest_events"])
#                     cols = ["event_time_dt", "event_ts", "spark_ts", "source_id", "frame_id", "emotion", "score", "e2e_latency_ms"]
#                     visible_cols = [c for c in cols if c in df.columns]
#                     df_view = df.sort_values(ts_base, ascending=False) if (ts_base and ts_base in df.columns) else df.copy()
#                     st.dataframe(df_view[visible_cols].head(100), use_container_width=True, height=380)

#                 with right:
#                     st.subheader(t["distribution"])
#                     if "emotion" in df.columns:
#                         emo_counts = df["emotion"].astype(str).str.lower().value_counts().reset_index()
#                         emo_counts.columns = ["emotion", "count"]
#                         fig_bar = px.bar(
#                             emo_counts,
#                             x="emotion",
#                             y="count",
#                             color="emotion",
#                             color_discrete_map=EMO_COLOR_MAP,
#                         )
#                         fig_bar.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=380, showlegend=False)
#                         st.plotly_chart(fig_bar, use_container_width=True)
#                     else:
#                         st.info(t["no_emotion"])

#                 st.subheader(t["timeline"])
#                 if ts_base is not None and "emotion" in df.columns:
#                     tmp = df.dropna(subset=[ts_base, "emotion"]).copy()
#                     tmp["minute"] = pd.to_datetime(tmp[ts_base], errors="coerce", utc=True).dt.floor("min")
#                     tmp = tmp.dropna(subset=["minute"])
#                     if tmp.empty:
#                         st.info(t["timeline_needs"])
#                     else:
#                         counts = tmp.groupby(["minute", "emotion"]).size().reset_index(name="count")
#                         counts["emotion"] = counts["emotion"].astype(str).str.lower()
#                         fig = px.line(
#                             counts,
#                             x="minute",
#                             y="count",
#                             color="emotion",
#                             color_discrete_map=EMO_COLOR_MAP,
#                         )
#                         fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=320)
#                         st.plotly_chart(fig, use_container_width=True)
#                 else:
#                     st.info(t["timeline_needs"])

# # ============================================================
# # TAB 2: Live (WebRTC)
# # ============================================================
# with tab_live:
#     render_topbar(t["tab_live"])

#     if not KAFKA_AVAILABLE:
#         st.warning(t["kafka_missing"])

#     if "live_on" not in st.session_state:
#         st.session_state.live_on = False

#     left_panel, video_panel = st.columns([0.42, 1.0], gap="large")

#     with left_panel:
#         st.caption(t["live_help"])

#         live_source_id = st.text_input(t["live_source_id"], value=str(LIVE_CFG.get("source_id", "cam_01")))

#         live_send_kafka = st.checkbox(
#             t["live_send_kafka"],
#             value=bool(LIVE_CFG.get("send_kafka", False)) and bool(KAFKA_AVAILABLE),
#             disabled=not KAFKA_AVAILABLE,
#         )

#         min_send_interval = st.number_input(
#             t["live_send_interval"], min_value=0.1, max_value=5.0,
#             value=float(LIVE_CFG.get("send_interval", 0.5)), step=0.1
#         )

#         kafka_bootstrap = st.text_input(
#             t["live_kafka_bootstrap"],
#             value=str(LIVE_CFG.get("kafka_bootstrap", "localhost:9092"))
#         )

#         kafka_topic = st.text_input(
#             t["live_topic"],
#             value=str(LIVE_CFG.get("kafka_topic", "test_topic"))
#         )

#         cam_w = st.number_input(t["live_width"], min_value=320, max_value=1920, value=640, step=160)
#         cam_h = st.number_input(t["live_height"], min_value=240, max_value=1080, value=480, step=120)

#         detect_every_n = st.number_input(t["live_detect_every"], min_value=1, max_value=10, value=3, step=1)
#         hold_boxes_ms = st.number_input(t["live_hold_ms"], min_value=0, max_value=2000, value=450, step=50)

#         c1, c2 = st.columns(2)
#         with c1:
#             if st.button(t["live_start"], use_container_width=True):
#                 st.session_state.live_on = True
#                 do_rerun()
#         with c2:
#             if st.button(t["live_stop"], use_container_width=True):
#                 st.session_state.live_on = False
#                 do_rerun()

#         st.markdown(f"**{t['live_status']}:** `{ 'running' if st.session_state.live_on else 'stopped' }`")

#         # Update live config for processor
#         with LIVE_CFG_LOCK:
#             LIVE_CFG.update({
#                 "source_id": live_source_id,
#                 "send_kafka": bool(live_send_kafka),
#                 "send_interval": float(min_send_interval),
#                 "kafka_bootstrap": str(kafka_bootstrap),
#                 "kafka_topic": str(kafka_topic),
#                 "detect_every_n": int(detect_every_n),
#                 "hold_boxes_ms": int(hold_boxes_ms),
#             })

#     with video_panel:
#         if st.session_state.live_on:
#             rtc_configuration = RTCConfiguration(
#                 {"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]}
#             )

#             webrtc_streamer(
#                 key="live_webrtc",
#                 mode=WebRtcMode.SENDRECV,
#                 rtc_configuration=rtc_configuration,
#                 media_stream_constraints={
#                     "video": {
#                         "width": {"ideal": int(cam_w)},
#                         "height": {"ideal": int(cam_h)},
#                         "frameRate": {"ideal": 30},
#                     },
#                     "audio": False,
#                 },
#                 video_processor_factory=FaceKafkaWebRTCProcessor,
#                 async_processing=True,
#             )
#             st.info(t["expected"])
#         else:
#             st.info("Live is stopped. Click Start Live.")

import os
import time
import json
import threading
from typing import Optional, Tuple, List, Dict

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

import cv2
import av
import onnxruntime as ort
from collections import deque
from pathlib import Path

from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh
from streamlit_webrtc import (
    webrtc_streamer,
    VideoProcessorBase,
    WebRtcMode,
    RTCConfiguration,
)

# ============================
# Paths / Env (robust project root)
# ============================
APP_DIR = Path(__file__).resolve().parent  # .../src/dashboard

def find_project_root(start: Path) -> Path:
    p = start
    for _ in range(10):
        if (p / "docker-compose.yml").exists() or (p / "models").exists():
            return p
        p = p.parent
    return start.parent

PROJECT_ROOT = find_project_root(APP_DIR)
load_dotenv()

# ============================
# Optional: DuckDB
# ============================
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except Exception:
    duckdb = None  # type: ignore
    DUCKDB_AVAILABLE = False

# ============================
# Optional: Kafka
# ============================
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except Exception:
    KafkaProducer = None  # type: ignore
    KAFKA_AVAILABLE = False

# ============================
# MediaPipe
# ============================
try:
    import mediapipe as mp
    from mediapipe import solutions as mp_solutions
    MEDIAPIPE_OK = True
except Exception:
    mp = None  # type: ignore
    mp_solutions = None  # type: ignore
    MEDIAPIPE_OK = False

# ============================
# Shared LIVE config
# ============================
LIVE_CFG_LOCK = threading.Lock()
LIVE_CFG = {
    "source_id": "cam_01",
    "send_kafka": False,
    "send_interval": 0.5,
    "kafka_bootstrap": "localhost:9092",
    "kafka_topic": "test_topic",

    # PERFORMANCE defaults (fast)
    "detect_every_n": 3,     # detect 1 out of N frames
    "hold_boxes_ms": 450,    # reuse last boxes for smoother UI

    # Model files
    "model_path": "models/emotion_vit_small_8cls.onnx",
    "labels_path": "models/emotions.txt",

    # Stability
    "ema_alpha": 0.65,
    "vote_window": 15,
    "min_display_score": 0.55,
    "change_confirm_frames": 6,
    "track_ttl_ms": 900,

    # Face detection
    "min_det_conf": 0.45,

    # Debug overlay
    "debug_overlay": False,
}

# ============================
# Helpers
# ============================
def softmax(x: np.ndarray) -> np.ndarray:
    x = x - np.max(x)
    e = np.exp(x)
    return e / (np.sum(e) + 1e-9)

def do_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass

def make_kafka_producer(bootstrap: str):
    if not KAFKA_AVAILABLE or not bootstrap:
        return None
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1,
        retries=1,
        linger_ms=10,
        request_timeout_ms=1500,
        api_version_auto_timeout_ms=1500,
        max_block_ms=1500,
        connections_max_idle_ms=30000,
    )

def iou_xywh(a, b) -> float:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    ax2, ay2 = ax + aw, ay + ah
    bx2, by2 = bx + bw, by + bh
    ix1 = max(ax, bx)
    iy1 = max(ay, by)
    ix2 = min(ax2, bx2)
    iy2 = min(ay2, by2)
    iw = max(0, ix2 - ix1)
    ih = max(0, iy2 - iy1)
    inter = iw * ih
    union = (aw * ah) + (bw * bh) - inter
    return float(inter / union) if union > 0 else 0.0

def draw_text(img_bgr: np.ndarray, text: str, y: int, color=(0, 255, 255)):
    cv2.putText(
        img_bgr, text[:120], (10, y),
        cv2.FONT_HERSHEY_SIMPLEX, 0.65, color, 2, cv2.LINE_AA
    )

# ============================
# ONNX Emotion Model (ViT)
# ============================
class EmotionViTOnnx:
    """
    Match training:
      - grayscale->RGB
      - resize 224
      - normalize (x-0.5)/0.5
    """
    def __init__(self, model_path: str, labels_path: str):
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"ONNX model not found: {model_path}")
        if not os.path.exists(labels_path):
            raise FileNotFoundError(f"labels not found: {labels_path}")

        self.labels = [l.strip() for l in open(labels_path, "r", encoding="utf-8") if l.strip()]
        if len(self.labels) == 0:
            raise RuntimeError("labels file is empty (emotions.txt)")

        self.sess = self._make_session(model_path)
        self.input_name = self.sess.get_inputs()[0].name
        self.output_name = self.sess.get_outputs()[0].name

    def _make_session(self, model_path: str) -> ort.InferenceSession:
        providers = ort.get_available_providers()
        if "CUDAExecutionProvider" in providers:
            prov = ["CUDAExecutionProvider", "CPUExecutionProvider"]
        elif "DmlExecutionProvider" in providers:
            prov = ["DmlExecutionProvider", "CPUExecutionProvider"]
        else:
            prov = ["CPUExecutionProvider"]

        so = ort.SessionOptions()
        so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        return ort.InferenceSession(model_path, sess_options=so, providers=prov)

    def preprocess(self, face_bgr: np.ndarray) -> np.ndarray:
        img = cv2.resize(face_bgr, (224, 224), interpolation=cv2.INTER_AREA)
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        img = cv2.cvtColor(gray, cv2.COLOR_GRAY2RGB).astype(np.float32) / 255.0
        img = (img - 0.5) / 0.5
        img = np.transpose(img, (2, 0, 1))[None, ...]
        return img

    def predict_probs(self, face_bgr: np.ndarray) -> np.ndarray:
        x = self.preprocess(face_bgr)
        logits = self.sess.run([self.output_name], {self.input_name: x})[0][0]
        probs = softmax(logits.astype(np.float32))
        return probs

# ============================
# Per-face track smoother
# ============================
class TrackState:
    def __init__(self, track_id: int, vote_window: int, alpha: float):
        self.track_id = track_id
        self.last_seen = time.time()
        self.bbox_xywh = (0, 0, 1, 1)

        self.ema: Optional[np.ndarray] = None
        self.alpha = alpha
        self.vote = deque(maxlen=max(3, vote_window))

        self.stable_label = ""
        self.stable_score = 0.0

        self.pending_label = ""
        self.pending_count = 0

    def update_params(self, vote_window: int, alpha: float):
        self.alpha = alpha
        new_maxlen = max(3, vote_window)
        if self.vote.maxlen != new_maxlen:
            self.vote = deque(self.vote, maxlen=new_maxlen)

    def update(self, probs: np.ndarray, labels: List[str], min_score: float, confirm_frames: int):
        if self.ema is None:
            self.ema = probs.copy()
        else:
            self.ema = (self.alpha * probs) + ((1.0 - self.alpha) * self.ema)

        idx = int(np.argmax(self.ema))
        label_ema = labels[idx] if idx < len(labels) else f"class_{idx}"
        score_ema = float(np.max(self.ema))

        self.vote.append(label_ema)
        label_vote = max(set(self.vote), key=self.vote.count)

        candidate = label_vote
        candidate_score = score_ema

        if self.stable_label == "":
            self.stable_label = candidate
            self.stable_score = candidate_score
            return self.stable_label, self.stable_score

        if candidate == self.stable_label:
            self.stable_score = candidate_score
            self.pending_label = ""
            self.pending_count = 0
            return self.stable_label, self.stable_score

        if candidate_score < float(min_score):
            return self.stable_label, self.stable_score

        if self.pending_label != candidate:
            self.pending_label = candidate
            self.pending_count = 1
        else:
            self.pending_count += 1

        if self.pending_count >= int(confirm_frames):
            self.stable_label = candidate
            self.stable_score = candidate_score
            self.pending_label = ""
            self.pending_count = 0

        return self.stable_label, self.stable_score

# ============================
# WebRTC Processor (fast + stable)
# ============================
class FaceKafkaWebRTCProcessor(VideoProcessorBase):
    def __init__(self):
        self.frame_id = 0
        self.last_sent_ts = 0.0
        self.last_flush_ts = 0.0

        self._producer = None
        self._producer_bootstrap = ""
        self._producer_topic = ""

        self.model: Optional[EmotionViTOnnx] = None
        self.model_path = ""
        self.labels_path = ""

        self.tracks: Dict[int, TrackState] = {}
        self.next_track_id = 1

        self._last_boxes = []
        self._last_boxes_ts = 0.0

        self.last_err = ""
        self._fps_t0 = time.time()
        self._fps_n = 0
        self._fps = 0.0

        if not MEDIAPIPE_OK:
            self.mp_face = None
            self.last_err = "MediaPipe not available"
        else:
            self.mp_face = mp_solutions.face_detection.FaceDetection(
                model_selection=0,
                min_detection_confidence=0.45
            )

    def _ensure_producer(self, enabled: bool, bootstrap: str, topic: str):
        if not enabled or not KAFKA_AVAILABLE:
            if self._producer is not None:
                try: self._producer.flush(timeout=0.2)
                except Exception: pass
                try: self._producer.close(timeout=1)
                except Exception: pass
            self._producer = None
            self._producer_bootstrap = ""
            self._producer_topic = ""
            return

        if (self._producer is None) or (self._producer_bootstrap != bootstrap) or (self._producer_topic != topic):
            self._producer = make_kafka_producer(bootstrap)
            self._producer_bootstrap = bootstrap
            self._producer_topic = topic

    def _ensure_model(self, model_path: str, labels_path: str):
        mpth = Path(model_path)
        lpth = Path(labels_path)
        if not mpth.is_absolute():
            mpth = (PROJECT_ROOT / mpth).resolve()
        if not lpth.is_absolute():
            lpth = (PROJECT_ROOT / lpth).resolve()

        mpth_s = str(mpth)
        lpth_s = str(lpth)

        if self.model is None or self.model_path != mpth_s or self.labels_path != lpth_s:
            self.model = EmotionViTOnnx(mpth_s, lpth_s)
            self.model_path = mpth_s
            self.labels_path = lpth_s
            self.tracks = {}
            self.next_track_id = 1

    def _mp_detect(self, img_bgr: np.ndarray):
        if self.mp_face is None:
            return []
        img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
        res = self.mp_face.process(img_rgb)
        boxes = []
        if res.detections:
            h, w = img_bgr.shape[:2]
            for det in res.detections:
                b = det.location_data.relative_bounding_box
                x0 = int(b.xmin * w)
                y0 = int(b.ymin * h)
                bw = int(b.width * w)
                bh = int(b.height * h)
                score = float(det.score[0] if det.score else 0.8)
                boxes.append((x0, y0, bw, bh, score))
        return boxes

    def _assign_tracks(self, boxes_xywh: List[Tuple[int, int, int, int]], now: float, ttl_s: float) -> Dict[int, Tuple[int, int, int, int]]:
        stale = [tid for tid, tr in self.tracks.items() if (now - tr.last_seen) > ttl_s]
        for tid in stale:
            del self.tracks[tid]

        assigned: Dict[int, Tuple[int, int, int, int]] = {}
        used_tracks = set()

        for bx in boxes_xywh:
            best_tid = None
            best_i = 0.0
            for tid, tr in self.tracks.items():
                if tid in used_tracks:
                    continue
                i = iou_xywh(bx, tr.bbox_xywh)
                if i > best_i:
                    best_i = i
                    best_tid = tid

            if best_tid is not None and best_i >= 0.25:
                assigned[best_tid] = bx
                used_tracks.add(best_tid)
            else:
                tid = self.next_track_id
                self.next_track_id += 1
                with LIVE_CFG_LOCK:
                    cfg = dict(LIVE_CFG)
                vote_window = int(cfg.get("vote_window", 15))
                alpha = float(cfg.get("ema_alpha", 0.65))
                self.tracks[tid] = TrackState(track_id=tid, vote_window=vote_window, alpha=alpha)
                assigned[tid] = bx
                used_tracks.add(tid)

        for tid, bx in assigned.items():
            tr = self.tracks[tid]
            tr.bbox_xywh = bx
            tr.last_seen = now

        return assigned

    def recv(self, frame: av.VideoFrame) -> av.VideoFrame:
        img_bgr = frame.to_ndarray(format="bgr24")
        H, W = img_bgr.shape[:2]
        self.frame_id += 1
        now = time.time()

        # FPS
        self._fps_n += 1
        dt = now - self._fps_t0
        if dt >= 1.0:
            self._fps = self._fps_n / dt
            self._fps_n = 0
            self._fps_t0 = now

        with LIVE_CFG_LOCK:
            cfg = dict(LIVE_CFG)

        debug_overlay = bool(cfg.get("debug_overlay", False))
        detect_every_n = int(cfg.get("detect_every_n", 3))
        hold_boxes_ms = int(cfg.get("hold_boxes_ms", 450))

        alpha = float(cfg.get("ema_alpha", 0.65))
        vote_window = int(cfg.get("vote_window", 15))
        min_display_score = float(cfg.get("min_display_score", 0.55))
        change_confirm_frames = int(cfg.get("change_confirm_frames", 6))
        ttl_s = max(0.2, int(cfg.get("track_ttl_ms", 900)) / 1000.0)

        source_id = str(cfg.get("source_id", "cam_01"))
        send_kafka = bool(cfg.get("send_kafka", False))
        send_interval = float(cfg.get("send_interval", 0.5))
        kafka_bootstrap = str(cfg.get("kafka_bootstrap", "localhost:9092"))
        kafka_topic = str(cfg.get("kafka_topic", "test_topic"))

        model_path = str(cfg.get("model_path", "models/emotion_vit_small_8cls.onnx"))
        labels_path = str(cfg.get("labels_path", "models/emotions.txt"))

        try:
            self._ensure_model(model_path, labels_path)
        except Exception as e:
            self.last_err = f"MODEL ERROR: {type(e).__name__}: {e}"
            if debug_overlay:
                draw_text(img_bgr, self.last_err, 25, (0, 0, 255))
            return av.VideoFrame.from_ndarray(img_bgr, format="bgr24")

        try:
            self._ensure_producer(send_kafka, kafka_bootstrap, kafka_topic)
        except Exception as e:
            self.last_err = f"KAFKA ERROR: {type(e).__name__}: {e}"
            self._producer = None

        # Detect 1/N + hold
        if (self.frame_id % max(1, detect_every_n)) == 0:
            dets = self._mp_detect(img_bgr)
            self._last_boxes = dets
            self._last_boxes_ts = now
        else:
            age_ms = (now - self._last_boxes_ts) * 1000.0
            dets = self._last_boxes if (self._last_boxes and age_ms <= max(0, hold_boxes_ms)) else []

        boxes_xywh = []
        for x, y, bw, bh, conf in dets:
            x0 = max(0, x)
            y0 = max(0, y)
            x1 = min(W, x0 + bw)
            y1 = min(H, y0 + bh)
            if x1 > x0 and y1 > y0:
                boxes_xywh.append((x0, y0, x1 - x0, y1 - y0))

        assigned = self._assign_tracks(boxes_xywh, now=now, ttl_s=ttl_s)

        # Update smoothing params
        for tr in self.tracks.values():
            tr.update_params(vote_window=vote_window, alpha=alpha)

        if debug_overlay:
            draw_text(img_bgr, f"FPS:{self._fps:.1f} faces:{len(assigned)}", 25, (0, 255, 255))
            if self.last_err:
                draw_text(img_bgr, self.last_err, 50, (0, 0, 255))

        # Predict per face
        for tid, (x0, y0, bw, bh) in assigned.items():
            x1 = min(W, x0 + bw)
            y1 = min(H, y0 + bh)
            if x1 <= x0 or y1 <= y0:
                continue

            face = img_bgr[y0:y1, x0:x1]

            try:
                probs = self.model.predict_probs(face)  # type: ignore
            except Exception as e:
                self.last_err = f"ONNX RUN ERROR: {type(e).__name__}: {e}"
                continue

            tr = self.tracks.get(tid)
            if tr is None:
                continue

            label_stable, score_stable = tr.update(
                probs=probs,
                labels=self.model.labels,  # type: ignore
                min_score=min_display_score,
                confirm_frames=change_confirm_frames,
            )

            cv2.rectangle(img_bgr, (x0, y0), (x1, y1), (0, 255, 0), 2)
            cv2.putText(
                img_bgr,
                f"#{tid} {label_stable} ({score_stable:.2f})",
                (x0, max(24, y0 - 10)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.7,
                (0, 255, 0),
                2,
            )

            # Kafka throttle
            if self._producer is not None and (now - self.last_sent_ts) >= send_interval:
                self.last_sent_ts = now
                event = {
                    "source_id": source_id,
                    "frame_id": int(self.frame_id),
                    "face_id": int(tid),
                    "producer_ts": float(now),
                    "emotion": str(label_stable),
                    "score": float(score_stable),
                    "bbox_x": int(x0),
                    "bbox_y": int(y0),
                    "bbox_w": int(x1 - x0),
                    "bbox_h": int(y1 - y0),
                }
                try:
                    self._producer.send(kafka_topic, event)
                except Exception:
                    pass

                if (now - self.last_flush_ts) > 1.2:
                    self.last_flush_ts = now
                    try:
                        self._producer.flush(timeout=0.2)
                    except Exception:
                        pass

        return av.VideoFrame.from_ndarray(img_bgr, format="bgr24")

# ============================
# Analytics helpers
# ============================
@st.cache_data(ttl=2)
def load_parquet_folder(path: str) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()

    p = Path(path)
    if not p.is_absolute() and not p.exists():
        p = (PROJECT_ROOT / p).resolve()
    if not p.exists():
        return pd.DataFrame()

    if DUCKDB_AVAILABLE:
        try:
            pp = str(p).replace("\\", "/").rstrip("/")
            glob1 = pp + "/*.parquet"
            glob2 = pp + "/*/*.parquet"
            glob3 = pp + "/*/*/*.parquet"
            con = duckdb.connect(database=":memory:")
            df = con.execute(
                f"""
                SELECT * FROM read_parquet(
                ['{glob1}','{glob2}','{glob3}'],
                hive_partitioning=true,
                union_by_name=true
                )
                """
            ).df()
            con.close()
            return df
        except Exception:
            pass

    try:
        import pyarrow.dataset as ds
        dataset = ds.dataset(str(p), format="parquet", partitioning="hive")
        return dataset.to_table().to_pandas()
    except Exception:
        try:
            return pd.read_parquet(str(p), engine="pyarrow")
        except Exception:
            return pd.DataFrame()

def percentile_safe(series: pd.Series, q: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return np.nan
    return float(np.percentile(s, q))

def pick_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time", "producer_ts"]
    for c in candidates:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
            if not df[c].isna().all():
                return c
    return None

# ============================
# I18N + CSS
# ============================
I18N = {
    "en": {
        "sidebar_title": "Controls",
        "tab_analytics": "Analytics (Parquet)",
        "tab_live": "Live Webcam (WebRTC)",
        "data_path": "Parquet folder",
        "refresh": "Refresh",
        "auto_refresh": "Auto refresh (seconds)",
        "source_filter": "Source (camera)",
        "time_filter": "Time filter (last N minutes)",
        "min_score": "Min score",
        "lat_p50": "E2E Latency p50",
        "lat_p95": "E2E Latency p95",
        "proc_p50": "Processing p50",
        "proc_p95": "Processing p95",
        "events_rate": "Events (rows)",
        "latest_events": "Latest Events",
        "distribution": "Emotion Distribution",
        "timeline": "Detections Over Time",
        "no_data": "No data found in the selected folder.",
        "note": "Tip: keep Spark job running to see live updates.",
        "no_ts_found": "No usable timestamp column found.",
        "no_emotion": "No emotion column found.",
        "timeline_needs": "Timeline needs a timestamp column + emotion column.",
        "no_events_in_window": "No events in selected time window.",
        "last_updated": "Last updated",
        "live_help": "WebRTC + MediaPipe + ViT ONNX (stable) + optional Kafka",
        "live_source_id": "source_id",
        "live_send_kafka": "Send events to Kafka",
        "live_send_interval": "Min seconds between sends",
        "live_kafka_bootstrap": "Kafka bootstrap (Windows)",
        "live_topic": "Kafka topic",
        "live_width": "Camera width",
        "live_height": "Camera height",
        "live_detect_every": "Detect faces every N frames",
        "live_hold_ms": "Hold boxes (ms)",
        "live_model_path": "ONNX model path",
        "live_labels_path": "Labels path (emotions.txt)",
        "live_ema_alpha": "EMA alpha",
        "live_vote_window": "Vote window (frames)",
        "live_min_display_score": "Min score to switch label",
        "live_change_confirm": "Confirm frames to switch",
        "live_track_ttl": "Track TTL (ms)",
        "live_debug": "Debug overlay",
        "live_start": "Start Live",
        "live_stop": "Stop Live",
        "live_status": "Status",
        "expected": "✅ Expected: smooth video + stable labels.",
        "kafka_missing": "Kafka is not available (pip install kafka-python).",
    },
    "ru": {
        "sidebar_title": "Управление",
        "tab_analytics": "Аналитика (Parquet)",
        "tab_live": "Веб-камера (WebRTC)",
        "data_path": "Папка Parquet",
        "refresh": "Обновить",
        "auto_refresh": "Автообновление (сек.)",
        "source_filter": "Источник (камера)",
        "time_filter": "Фильтр времени (последние N минут)",
        "min_score": "Мин. уверенность (score)",
        "lat_p50": "E2E задержка p50",
        "lat_p95": "E2E задержка p95",
        "proc_p50": "Обработка p50",
        "proc_p95": "Обработка p95",
        "events_rate": "События (строки)",
        "latest_events": "Последние события",
        "distribution": "Распределение эмоций",
        "timeline": "Детекции во времени",
        "no_data": "В выбранной папке нет данных.",
        "note": "Совет: оставь Spark job запущенным, чтобы видеть обновления.",
        "no_ts_found": "Не найдена подходящая колонка времени.",
        "no_emotion": "Колонка emotion не найдена.",
        "timeline_needs": "Для таймлайна нужны колонка времени и колонка emotion.",
        "no_events_in_window": "В выбранном окне времени нет событий.",
        "last_updated": "Последнее обновление",
        "live_help": "WebRTC + MediaPipe + ViT ONNX (стабильно) + Kafka (опц.)",
        "live_source_id": "source_id",
        "live_send_kafka": "Отправлять события в Kafka",
        "live_send_interval": "Мин. секунды между отправками",
        "live_kafka_bootstrap": "Kafka bootstrap (Windows)",
        "live_topic": "Kafka topic",
        "live_width": "Ширина камеры",
        "live_height": "Высота камеры",
        "live_detect_every": "Детект каждые N кадров",
        "live_hold_ms": "Держать боксы (мс)",
        "live_model_path": "Путь к ONNX модели",
        "live_labels_path": "Путь к labels (emotions.txt)",
        "live_ema_alpha": "EMA alpha",
        "live_vote_window": "Окно голосования (кадры)",
        "live_min_display_score": "Мин. score для смены",
        "live_change_confirm": "Кадров для подтверждения",
        "live_track_ttl": "TTL трека (мс)",
        "live_debug": "Debug overlay",
        "live_start": "Старт Live",
        "live_stop": "Стоп Live",
        "live_status": "Статус",
        "expected": "✅ Ожидаемо: плавное видео + стабильные эмоции.",
        "kafka_missing": "Kafka недоступен (pip install kafka-python).",
    },
}

APP_CSS = r"""
<style>
.block-container { max-width: none !important; padding-top: 0.70rem; padding-left: 2.2rem; padding-right: 2.2rem; }
#MainMenu {visibility: hidden;} footer {visibility: hidden;}
</style>
"""

EMO_COLOR_MAP: Dict[str, str] = {
    "anger": "#ff7f0e",
    "contempt": "#8c564b",
    "disgust": "#2ca02c",
    "fear": "#bcbd22",
    "happiness": "#1f77b4",
    "neutral": "#7f7f7f",
    "sadness": "#d62728",
    "surprise": "#9467bd",
}

# ============================
# Streamlit UI
# ============================
st.set_page_config(page_title="Emotion Dashboard", layout="wide", initial_sidebar_state="expanded")
st.markdown(APP_CSS, unsafe_allow_html=True)

lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
t = I18N[lang]
st.sidebar.header(t["sidebar_title"])

st.markdown(
    """
    <div style="padding:14px 16px; border-radius:14px; background:#0f1420; color:#fff; margin: 8px 0 12px 0;">
      <div style="font-size:26px; font-weight:900;">Real-Time Emotion Analytics Dashboard</div>
      <div style="opacity:0.78; font-weight:700; margin-top:6px;">
        Spark → Parquet (Analytics) · Webcam → Kafka (Live)
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

tab_analytics, tab_live = st.tabs([t["tab_analytics"], t["tab_live"]])

def render_topbar(title: str):
    st.markdown(
        f"""
        <div style="padding:10px 14px; border-radius:12px; background:#243a66; color:white; margin-bottom:10px;">
          <div style="font-size:22px; font-weight:900;">{title}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ============================================================
# TAB 1: Analytics (FULL)
# ============================================================
with tab_analytics:
    render_topbar(t["tab_analytics"])

    default_path = os.getenv("PARQUET_PATH", "data/parquet_events_v2")
    parquet_path = st.sidebar.text_input(t["data_path"], value=default_path)

    min_minutes = st.sidebar.number_input(t["time_filter"], min_value=1, max_value=1440, value=60, step=5)
    min_score = st.sidebar.slider(t["min_score"], min_value=0.0, max_value=1.0, value=0.5, step=0.05)

    refresh_now = st.sidebar.button(t["refresh"])
    auto_refresh_s = st.sidebar.number_input(t["auto_refresh"], min_value=0, max_value=3600, value=10, step=1)
    st.sidebar.caption(t["note"])

    if refresh_now:
        st.cache_data.clear()

    if int(auto_refresh_s) > 0:
        st_autorefresh(interval=int(auto_refresh_s) * 1000, key="autorefresh_analytics")

    df = load_parquet_folder(parquet_path)
    if df.empty:
        st.warning(t["no_data"])
    else:
        if "event_time" in df.columns and "event_time_dt" not in df.columns:
            df["event_time_dt"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)

        for c in ["event_ts", "spark_ts", "event_time_dt", "producer_ts"]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

        ts_base = pick_timestamp_column(df)
        if ts_base is None:
            st.info(t["no_ts_found"])

        if ts_base is not None:
            now_ts = df[ts_base].max()
            if pd.notna(now_ts):
                cutoff = now_ts - pd.Timedelta(minutes=int(min_minutes))
                df = df[df[ts_base] >= cutoff]

        if df.empty:
            st.warning(t["no_events_in_window"])
        else:
            if "score" in df.columns:
                df = df[pd.to_numeric(df["score"], errors="coerce").fillna(0) >= float(min_score)]

            source_col = "source_id" if "source_id" in df.columns else None
            if source_col:
                sources = sorted(df[source_col].dropna().unique().tolist())
                selected = st.sidebar.selectbox(t["source_filter"], ["ALL"] + sources, index=0, key="src_filter")
                if selected != "ALL":
                    df = df[df[source_col] == selected]

            if df.empty:
                st.warning(t["no_events_in_window"])
            else:
                lat_p50 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 50)
                lat_p95 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 95)
                proc_p50 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 50)
                proc_p95 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 95)

                def fmt(x: float) -> str:
                    return "—" if np.isnan(x) else f"{x:.0f}"

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric(t["lat_p50"], fmt(lat_p50))
                c2.metric(t["lat_p95"], fmt(lat_p95))
                c3.metric(t["proc_p50"], fmt(proc_p50))
                c4.metric(t["proc_p95"], fmt(proc_p95))
                c5.metric(t["events_rate"], f"{len(df)}")

                st.caption(f"{t['last_updated']}: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

                left, right = st.columns([1.35, 1.0], gap="large")

                with left:
                    st.subheader(t["latest_events"])
                    cols = ["event_time_dt", "event_ts", "spark_ts", "producer_ts", "source_id", "frame_id", "face_id", "emotion", "score", "e2e_latency_ms"]
                    visible_cols = [c for c in cols if c in df.columns]
                    df_view = df.sort_values(ts_base, ascending=False) if (ts_base and ts_base in df.columns) else df.copy()
                    st.dataframe(df_view[visible_cols].head(200), use_container_width=True, height=380)

                with right:
                    st.subheader(t["distribution"])
                    if "emotion" in df.columns:
                        emo_counts = df["emotion"].astype(str).str.lower().value_counts().reset_index()
                        emo_counts.columns = ["emotion", "count"]
                        fig_bar = px.bar(
                            emo_counts, x="emotion", y="count",
                            color="emotion", color_discrete_map=EMO_COLOR_MAP,
                        )
                        fig_bar.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=380, showlegend=False)
                        st.plotly_chart(fig_bar, use_container_width=True)
                    else:
                        st.info(t["no_emotion"])

                st.subheader(t["timeline"])
                if ts_base is not None and "emotion" in df.columns:
                    tmp = df.dropna(subset=[ts_base, "emotion"]).copy()
                    tmp["minute"] = pd.to_datetime(tmp[ts_base], errors="coerce", utc=True).dt.floor("min")
                    tmp = tmp.dropna(subset=["minute"])
                    if tmp.empty:
                        st.info(t["timeline_needs"])
                    else:
                        counts = tmp.groupby(["minute", "emotion"]).size().reset_index(name="count")
                        counts["emotion"] = counts["emotion"].astype(str).str.lower()
                        fig = px.line(
                            counts, x="minute", y="count",
                            color="emotion", color_discrete_map=EMO_COLOR_MAP,
                        )
                        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=320)
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(t["timeline_needs"])

# ============================================================
# TAB 2: Live (FAST)
# ============================================================
with tab_live:
    render_topbar(t["tab_live"])

    if not MEDIAPIPE_OK:
        st.error("MediaPipe not available in this environment (but you installed 0.10.14, so it should be OK).")

    if not KAFKA_AVAILABLE:
        st.warning(t["kafka_missing"])

    if "live_on" not in st.session_state:
        st.session_state.live_on = False

    left_panel, video_panel = st.columns([0.42, 1.0], gap="large")

    with left_panel:
        st.caption(t["live_help"])

        live_source_id = st.text_input(t["live_source_id"], value=str(LIVE_CFG.get("source_id", "cam_01")))
        live_send_kafka = st.checkbox(
            t["live_send_kafka"],
            value=bool(LIVE_CFG.get("send_kafka", False)) and bool(KAFKA_AVAILABLE),
            disabled=not KAFKA_AVAILABLE,
        )

        min_send_interval = st.number_input(
            t["live_send_interval"], min_value=0.1, max_value=5.0,
            value=float(LIVE_CFG.get("send_interval", 0.5)), step=0.1
        )
        kafka_bootstrap = st.text_input(t["live_kafka_bootstrap"], value=str(LIVE_CFG.get("kafka_bootstrap", "localhost:9092")))
        kafka_topic = st.text_input(t["live_topic"], value=str(LIVE_CFG.get("kafka_topic", "test_topic")))

        cam_w = st.number_input(t["live_width"], min_value=320, max_value=1920, value=640, step=160)
        cam_h = st.number_input(t["live_height"], min_value=240, max_value=1080, value=480, step=120)

        detect_every_n = st.number_input(t["live_detect_every"], min_value=1, max_value=10, value=int(LIVE_CFG.get("detect_every_n", 3)), step=1)
        hold_boxes_ms = st.number_input(t["live_hold_ms"], min_value=0, max_value=2000, value=int(LIVE_CFG.get("hold_boxes_ms", 450)), step=50)

        st.markdown("---")
        model_path = st.text_input(t["live_model_path"], value=str(LIVE_CFG.get("model_path", "models/emotion_vit_small_8cls.onnx")))
        labels_path = st.text_input(t["live_labels_path"], value=str(LIVE_CFG.get("labels_path", "models/emotions.txt")))

        ema_alpha = st.slider(t["live_ema_alpha"], 0.05, 0.95, float(LIVE_CFG.get("ema_alpha", 0.65)), 0.05)
        vote_window = st.slider(t["live_vote_window"], 3, 45, int(LIVE_CFG.get("vote_window", 15)), 1)
        min_display_score = st.slider(t["live_min_display_score"], 0.10, 0.95, float(LIVE_CFG.get("min_display_score", 0.55)), 0.05)
        change_confirm_frames = st.slider(t["live_change_confirm"], 1, 20, int(LIVE_CFG.get("change_confirm_frames", 6)), 1)
        track_ttl_ms = st.slider(t["live_track_ttl"], 200, 3000, int(LIVE_CFG.get("track_ttl_ms", 900)), 50)

        debug_overlay = st.checkbox(t["live_debug"], value=bool(LIVE_CFG.get("debug_overlay", False)))

        c1, c2 = st.columns(2)
        with c1:
            if st.button(t["live_start"], use_container_width=True):
                st.session_state.live_on = True
                do_rerun()
        with c2:
            if st.button(t["live_stop"], use_container_width=True):
                st.session_state.live_on = False
                do_rerun()

        st.markdown(f"**{t['live_status']}:** `{ 'running' if st.session_state.live_on else 'stopped' }`")

        with LIVE_CFG_LOCK:
            LIVE_CFG.update({
                "source_id": live_source_id,
                "send_kafka": bool(live_send_kafka),
                "send_interval": float(min_send_interval),
                "kafka_bootstrap": str(kafka_bootstrap),
                "kafka_topic": str(kafka_topic),
                "detect_every_n": int(detect_every_n),
                "hold_boxes_ms": int(hold_boxes_ms),
                "model_path": str(model_path),
                "labels_path": str(labels_path),
                "ema_alpha": float(ema_alpha),
                "vote_window": int(vote_window),
                "min_display_score": float(min_display_score),
                "change_confirm_frames": int(change_confirm_frames),
                "track_ttl_ms": int(track_ttl_ms),
                "debug_overlay": bool(debug_overlay),
            })

    with video_panel:
        if st.session_state.live_on:
            rtc_configuration = RTCConfiguration({"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]})

            webrtc_streamer(
                key="live_webrtc",
                mode=WebRtcMode.SENDRECV,
                rtc_configuration=rtc_configuration,
                media_stream_constraints={
                    "video": {
                        "width": {"ideal": int(cam_w)},
                        "height": {"ideal": int(cam_h)},
                        "frameRate": {"ideal": 30},
                    },
                    "audio": False,
                },
                video_processor_factory=FaceKafkaWebRTCProcessor,
                async_processing=True,   # ✅ back to fast
            )
            st.info(t["expected"])
        else:
            st.info("Live is stopped. Click Start Live.")