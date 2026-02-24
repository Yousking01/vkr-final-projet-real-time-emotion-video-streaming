import os
import time
import json
import threading
from typing import Optional, Tuple, List, Dict

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

# Live webcam (stable)
import cv2

# ---------------------------
# Kafka (OPTIONAL) - SAFE
# ---------------------------
try:
    from kafka import KafkaProducer  # pip install kafka-python
    KAFKA_AVAILABLE = True
except Exception:
    KafkaProducer = None  # type: ignore
    KAFKA_AVAILABLE = False

load_dotenv()

# ---------------------------
# Small helper: rerun (compatible)
# ---------------------------
def do_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass

# ---------------------------
# I18N (EN/RU)
# ---------------------------
I18N = {
    "en": {
        "sidebar_title": "Controls",
        "language": "Language",
        "tab_analytics": "Analytics (Parquet)",
        "tab_live": "Live Webcam (Windows - OpenCV Stable)",
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
        "no_ts_found": "No usable timestamp column found. Showing rows without time filter/sorting.",
        "no_emotion": "No emotion column found.",
        "timeline_needs": "Timeline needs a timestamp column + emotion column.",
        "no_events_in_window": "No events in selected time window. Send messages to Kafka or increase the time window.",
        "last_updated": "Last updated",

        "live_help": "Stable OpenCV webcam (Windows) + face boxes + emotion label + optional Kafka send",
        "live_source_id": "source_id",
        "live_send_kafka": "Send events to Kafka",
        "live_send_interval": "Min seconds between sends",
        "live_kafka_bootstrap": "Kafka bootstrap",
        "live_topic": "Kafka topic",
        "live_camera_index": "Camera index",
        "live_width": "Camera width",
        "live_height": "Camera height",
        "live_fps_limit": "FPS limit (capture thread)",
        "live_start": "Start Live",
        "live_stop": "Stop Live",
        "live_status": "Status",
        "payload_fields": "Payload fields (JSON)",
        "expected": "✅ Expected: live frames with rectangles. If Kafka ON, Spark writes Parquet and it appears in Analytics.",
        "cam_not_started": "Camera not started. Click Start Live.",
        "cam_error": "Camera error",
        "kafka_missing": "Kafka is not available (install: pip install kafka-python). Live can run without Kafka.",
    },
    "ru": {
        "sidebar_title": "Управление",
        "language": "Язык",
        "tab_analytics": "Аналитика (Parquet)",
        "tab_live": "Веб-камера (Windows - OpenCV стабильно)",
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
        "no_ts_found": "Не найдена подходящая колонка времени. Показываю без фильтра/сортировки по времени.",
        "no_emotion": "Колонка emotion не найдена.",
        "timeline_needs": "Для таймлайна нужны колонка времени и колонка emotion.",
        "no_events_in_window": "В выбранном окне времени нет событий. Отправь сообщения в Kafka или увеличь окно.",
        "last_updated": "Последнее обновление",

        "live_help": "Стабильная веб-камера OpenCV (Windows) + рамки лиц + эмоции + отправка в Kafka",
        "live_source_id": "source_id",
        "live_send_kafka": "Отправлять события в Kafka",
        "live_send_interval": "Мин. секунды между отправками",
        "live_kafka_bootstrap": "Kafka bootstrap",
        "live_topic": "Kafka topic",
        "live_camera_index": "Индекс камеры",
        "live_width": "Ширина камеры",
        "live_height": "Высота камеры",
        "live_fps_limit": "Лимит FPS (поток)",
        "live_start": "Старт Live",
        "live_stop": "Стоп Live",
        "live_status": "Статус",
        "payload_fields": "Поля payload (JSON)",
        "expected": "✅ Ожидаемо: live-кадры с рамками. Если Kafka ON, Spark пишет Parquet и это видно в Analytics.",
        "cam_not_started": "Камера не запущена. Нажми Старт.",
        "cam_error": "Ошибка камеры",
        "kafka_missing": "Kafka недоступен (установи: pip install kafka-python). Live работает и без Kafka.",
    },
}

# ---------------------------
# Premium CSS
# ---------------------------
APP_CSS = r"""
<style>
.block-container { max-width: none !important; padding-top: 0.70rem; padding-left: 2.2rem; padding-right: 2.2rem; }
#MainMenu {visibility: hidden;} footer {visibility: hidden;}

section[data-testid="stSidebar"]{
  background: linear-gradient(180deg, #1b1e26 0%, #202533 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.06);
}
section[data-testid="stSidebar"] *{ color: rgba(255,255,255,0.92) !important; }
section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3 { color: rgba(255,255,255,0.95) !important; letter-spacing: 0.2px; }
section[data-testid="stSidebar"] label p{ color: rgba(255,255,255,0.72) !important; font-weight: 750 !important; }

section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea {
  background: rgba(0,0,0,0.30) !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  border-radius: 14px !important;
  padding: 10px 12px !important;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.05) !important;
}
section[data-testid="stSidebar"] div[data-baseweb="select"] > div{
  background: rgba(0,0,0,0.30) !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  border-radius: 14px !important;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.05) !important;
}

.app-header{
  width: 100%;
  overflow: visible;
  border-radius: 16px;
  background: radial-gradient(1200px 240px at 20% 0%, rgba(120,200,255,0.18), transparent 55%),
              radial-gradient(900px 240px at 85% 10%, rgba(139,92,246,0.16), transparent 55%),
              linear-gradient(180deg, #0f1420 0%, #0b101a 100%);
  border: 1px solid rgba(255,255,255,0.06);
  box-shadow: 0 18px 44px rgba(0,0,0,0.28);
  padding: 22px 20px 18px 20px;
  margin: 10px 0 12px 0;
}
.app-header .brand-title{ font-size: 28px; font-weight: 950; color: rgba(255,255,255,0.96); line-height: 1.15; margin: 0; padding: 0; }
.app-header .brand-sub{ font-size: 14px; font-weight: 750; color: rgba(255,255,255,0.72); margin-top: 6px; }

.topbar {
  width: 100%;
  background: linear-gradient(90deg, #1f2b46 0%, #243a66 60%, #1f2b46 100%);
  padding: 14px 18px;
  border-radius: 14px;
  color: #ffffff;
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 8px 0 12px 0;
  box-shadow: 0 14px 34px rgba(0,0,0,0.25);
}
.topbar .left { display:flex; gap:14px; align-items:center; }
.logoWrap{
  width: 26px; height: 26px; border-radius: 9px;
  background: rgba(255,255,255,0.10);
  border: 1px solid rgba(255,255,255,0.20);
  display:flex; align-items:center; justify-content:center;
}
.logo {
  width: 18px; height: 18px; border-radius: 7px;
  background: conic-gradient(from 210deg, #7bdcff, #8b5cf6, #22c55e, #7bdcff);
  box-shadow: 0 0 0 8px rgba(123,220,255,0.10);
}
.topbar .title { font-size: 24px; font-weight: 950; line-height: 1.05; }
.topbar .subtitle { font-size: 13px; opacity: 0.88; margin-top: 4px; }
.topbar .right { font-size: 12px; opacity: 0.85; }

div[data-testid="stTabs"] [role="tablist"]{ gap: 34px; border-bottom: 1px solid rgba(255,255,255,0.10); padding-bottom: 10px; }
div[data-testid="stTabs"] button[role="tab"]{
  background: transparent !important; border: none !important;
  padding: 8px 0 !important; margin: 0 !important;
  font-size: 22px !important; font-weight: 950 !important;
  color: rgba(255,255,255,0.34) !important;
  border-bottom: 4px solid transparent !important;
}
div[data-testid="stTabs"] button[role="tab"][aria-selected="true"]{
  color: #e11d48 !important;
  border-bottom: 4px solid #e11d48 !important;
}
div[data-testid="stTabs"] div[data-baseweb="tab-highlight"]{ display:none !important; }

.kpi-card{
  background: #ffffff; border: 1px solid rgba(15,23,42,0.08);
  border-radius: 14px; padding: 12px 14px;
  box-shadow: 0 8px 20px rgba(15,23,42,0.06);
  height: 78px;
}
.kpi-label{ font-size: 12px; color: rgba(15,23,42,0.70); font-weight: 900; margin-bottom: 6px; }
.kpi-value{ font-size: 30px; font-weight: 950; color: #0f172a; line-height: 1.0; }
.kpi-unit{ font-size: 14px; font-weight: 900; color: rgba(15,23,42,0.65); margin-left: 6px; }

.section-card{
  background:#ffffff; border:1px solid rgba(15,23,42,0.08);
  border-radius: 14px; padding: 12px 14px;
  box-shadow: 0 8px 20px rgba(15,23,42,0.06);
}
hr.soft{ border:none;height:1px; background: rgba(15,23,42,0.10); margin: 14px 0; }

.gray-banner{
  width: 100%; background: #f1f3f6;
  border: 1px solid rgba(15,23,42,0.08);
  border-radius: 12px; padding: 10px 12px;
  color: rgba(15,23,42,0.78); font-weight: 850;
  margin-top: 10px;
}
.blue-band{
  width: 100%;
  background: linear-gradient(90deg, #2b5aa8, #2f6bc7);
  color: white; border-radius: 12px;
  padding: 10px 12px; font-weight: 950;
  margin-bottom: 10px;
  box-shadow: 0 8px 18px rgba(43,90,168,0.18);
}
.left-panel{
  background:#f6f7fb;
  border: 1px solid rgba(15,23,42,0.08);
  border-radius: 14px;
  padding: 14px 14px 10px 14px;
}
.status-ok{
  background: rgba(34,197,94,0.12);
  border: 1px solid rgba(34,197,94,0.28);
  color:#166534;
  border-radius: 12px;
  padding: 10px 12px;
  font-weight: 950;
}
.status-bad{
  background: rgba(239,68,68,0.10);
  border: 1px solid rgba(239,68,68,0.25);
  color:#7f1d1d;
  border-radius: 12px;
  padding: 10px 12px;
  font-weight: 950;
}
.checklist{
  background: #ffffff;
  border:1px solid rgba(15,23,42,0.08);
  border-radius: 14px;
  padding: 10px 12px;
  margin-top: 10px;
}
.checklist .item{
  display:flex; gap:10px; align-items:center;
  padding: 6px 0;
  color: rgba(15,23,42,0.85);
  font-weight: 900;
}
.tick{
  width:18px;height:18px;border-radius:6px;
  display:inline-flex; align-items:center; justify-content:center;
  background: rgba(34,197,94,0.14);
  border: 1px solid rgba(34,197,94,0.30);
  color:#166534;
  font-size: 12px;
  font-weight: 950;
}
</style>
"""

# ---------------------------
# Parquet helpers
# ---------------------------
@st.cache_data(ttl=2)
def load_parquet_folder(path: str) -> pd.DataFrame:
    import pandas as pd
    if not path or not os.path.exists(path):
        return pd.DataFrame()

    try:
        import pyarrow.dataset as ds
        dataset = ds.dataset(path, format="parquet", partitioning="hive")
        table = dataset.to_table()
        return table.to_pandas()
    except Exception:
        # fallback
        try:
            return pd.read_parquet(path, engine="pyarrow")
        except Exception:
            return pd.DataFrame()

def percentile_safe(series: pd.Series, q: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return np.nan
    return float(np.percentile(s, q))

def pick_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time"]
    for c in candidates:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
            if not df[c].isna().all():
                return c
    return None

def style_latest_events(df: pd.DataFrame) -> "pd.io.formats.style.Styler":
    return (
        df.style
        .set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "#2b5aa8"),
                ("color", "white"),
                ("font-weight", "950"),
                ("border", "0px"),
                ("padding", "10px"),
            ]},
            {"selector": "tbody td", "props": [
                ("padding", "8px"),
                ("border-bottom", "1px solid rgba(15,23,42,0.10)"),
            ]},
            {"selector": "tbody tr:hover td", "props": [
                ("background-color", "rgba(59,130,246,0.06)"),
            ]},
        ])
        .hide(axis="index")
    )

# ---------------------------
# Kafka helpers (safe)
# ---------------------------
def make_kafka_producer(bootstrap: str):
    if not KAFKA_AVAILABLE:
        return None
    if not bootstrap:
        return None
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=2,
        acks=1,
        linger_ms=10,
    )

def safe_send_kafka(producer, topic: str, event: dict) -> Tuple[bool, str]:
    if producer is None:
        return False, "Kafka not available"
    try:
        producer.send(topic, event)
        return True, "sent"
    except Exception as e:
        return False, f"Kafka send failed: {e}"

# ---------------------------
# Emotion stub
# ---------------------------
EMO_LIST = ["happy", "sad", "angry", "neutral", "surprised", "fear"]

def infer_emotion_stub(face_rgb: np.ndarray) -> Tuple[str, float]:
    label = EMO_LIST[int(time.time()) % len(EMO_LIST)]
    score = 0.70 + 0.20 * ((int(time.time() * 10) % 5) / 5)
    return label, float(min(score, 0.95))

EMO_COLOR_MAP: Dict[str, str] = {
    "happy": "#1f77b4",
    "sad": "#d62728",
    "angry": "#ff7f0e",
    "neutral": "#7f7f7f",
    "surprised": "#9467bd",
    "fear": "#bcbd22",
}

# ---------------------------
# Face detection (OpenCV Haar)
# ---------------------------
@st.cache_resource
def get_haar_face_detector():
    cascade_path = os.path.join(cv2.data.haarcascades, "haarcascade_frontalface_default.xml")
    cascade = cv2.CascadeClassifier(cascade_path)
    if cascade.empty():
        raise RuntimeError(f"Failed to load Haar cascade: {cascade_path}")
    return cascade

def detect_faces_haar(cascade, frame_rgb: np.ndarray) -> List[Tuple[int, int, int, int, float]]:
    gray = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2GRAY)
    h, w = gray.shape[:2]

    target_w = 640
    scale = 1.0
    if w > target_w:
        scale = target_w / float(w)
        new_h = max(1, int(h * scale))
        gray_small = cv2.resize(gray, (target_w, new_h), interpolation=cv2.INTER_AREA)
    else:
        gray_small = gray

    faces = cascade.detectMultiScale(
        gray_small,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(40, 40),
    )

    boxes: List[Tuple[int, int, int, int, float]] = []
    inv = 1.0 / scale
    for (x, y, fw, fh) in faces:
        x0 = int(x * inv)
        y0 = int(y * inv)
        w0 = int(fw * inv)
        h0 = int(fh * inv)
        boxes.append((x0, y0, w0, h0, 0.80))
    return boxes

# ============================================================
# Stable OpenCV Live Worker (robust start/stop)
# ============================================================


# class OpenCVLiveWorker:
#     def __init__(self):
#         self.lock = threading.Lock()
#         self.stop_event = threading.Event()
#         self.thread: Optional[threading.Thread] = None

#         self.last_frame_bgr = None
#         self.status = "idle"
#         self.cam_error = ""
#         self.kafka_msg = ""

#         self.frame_id = 0
#         self.last_sent_ts = 0.0
#         self.last_flush_ts = 0.0

#         self._cap = None
#         self._producer = None

#         # perf / stabilité
#         self.display_max_width = 960
#         self.detect_every_n = 3          # détection visage 1 frame sur N
#         self.hold_boxes_ms = 450         # garder les boxes un peu pour éviter clignotement

#         self._last_boxes = []
#         self._last_boxes_ts = 0.0

#         self._backend_name = "DSHOW"     # DSHOW / MSMF / AUTO

#     def is_running(self) -> bool:
#         return self.thread is not None and self.thread.is_alive() and not self.stop_event.is_set()

#     def _try_open_capture(self, idx: int, width: int, height: int):
#         # Essaye DSHOW puis MSMF si besoin (AUTO essaie les deux)
#         backends = []
#         if self._backend_name == "DSHOW":
#             backends = [cv2.CAP_DSHOW]
#         elif self._backend_name == "MSMF":
#             backends = [cv2.CAP_MSMF]
#         else:
#             cand = []
#             if hasattr(cv2, "CAP_DSHOW"):
#                 cand.append(cv2.CAP_DSHOW)
#             if hasattr(cv2, "CAP_MSMF"):
#                 cand.append(cv2.CAP_MSMF)
#             cand.append(0)
#             backends = cand

#         for be in backends:
#             cap = cv2.VideoCapture(idx, be)

#             if not cap.isOpened():
#                 try:
#                     cap.release()
#                 except Exception:
#                     pass
#                 continue

#             # MJPG = souvent plus fluide
#             try:
#                 cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
#             except Exception:
#                 pass

#             # buffer minimal
#             try:
#                 cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
#             except Exception:
#                 pass

#             if width > 0:
#                 cap.set(cv2.CAP_PROP_FRAME_WIDTH, float(width))
#             if height > 0:
#                 cap.set(cv2.CAP_PROP_FRAME_HEIGHT, float(height))

#             # warmup
#             ok_any = False
#             for _ in range(20):
#                 if self.stop_event.is_set():
#                     break
#                 ok, fr = cap.read()
#                 if ok and fr is not None:
#                     ok_any = True
#                     break
#                 time.sleep(0.02)

#             if ok_any:
#                 return cap

#             try:
#                 cap.release()
#             except Exception:
#                 pass

#         return None

#     def start(
#         self,
#         camera_index: int,
#         width: int,
#         height: int,
#         fps_limit: float,
#         source_id: str,
#         send_kafka: bool,
#         min_send_interval: float,
#         kafka_bootstrap: str,
#         kafka_topic: str,
#         backend_name: str = "DSHOW",
#         detect_every_n: int = 3,
#         hold_boxes_ms: int = 450,
#     ):
#         if self.is_running():
#             return

#         self.stop_event.clear()
#         self.status = "starting"
#         self.cam_error = ""
#         self.kafka_msg = ""

#         self._backend_name = backend_name
#         self.detect_every_n = max(1, int(detect_every_n))
#         self.hold_boxes_ms = max(0, int(hold_boxes_ms))

#         with self.lock:
#             self.last_frame_bgr = None

#         self._cap = None
#         self._producer = None
#         self._last_boxes = []
#         self._last_boxes_ts = 0.0

#         cascade = get_haar_face_detector()

#         cam_idx = int(camera_index)
#         cam_w = int(width)
#         cam_h = int(height)
#         fps_lim = float(fps_limit)
#         src_id = str(source_id)

#         do_kafka = bool(send_kafka) and KAFKA_AVAILABLE
#         send_interval = float(min_send_interval)
#         bootstrap = str(kafka_bootstrap)
#         topic = str(kafka_topic)

#         def _loop():
#             cap = None
#             producer = None
#             try:
#                 cap = self._try_open_capture(cam_idx, cam_w, cam_h)
#                 self._cap = cap

#                 if cap is None:
#                     self.status = "camera_error"
#                     self.cam_error = (
#                         f"VideoCapture open failed (index={cam_idx}). "
#                         f"Try camera index 0/1/2. Close Zoom/Teams/Camera app."
#                     )
#                     return

#                 if do_kafka:
#                     try:
#                         producer = make_kafka_producer(bootstrap)
#                         self._producer = producer
#                         self.kafka_msg = f"Kafka producer OK ({bootstrap})" if producer else "Kafka producer: None"
#                     except Exception as e:
#                         producer = None
#                         self._producer = None
#                         self.kafka_msg = f"Kafka producer error: {e}"
#                 else:
#                     producer = None
#                     self._producer = None
#                     self.kafka_msg = "Kafka: disabled"

#                 self.status = "running"
#                 last_frame_time = 0.0
#                 frame_count = 0

#                 while not self.stop_event.is_set():
#                     # limite FPS
#                     now = time.time()
#                     if fps_lim and fps_lim > 0:
#                         min_dt = 1.0 / fps_lim
#                         dt = now - last_frame_time
#                         if dt < min_dt:
#                             time.sleep(min_dt - dt)
#                         last_frame_time = time.time()

#                     ok, frame_bgr = cap.read()
#                     if not ok or frame_bgr is None:
#                         if self.stop_event.is_set():
#                             break
#                         self.status = "camera_error"
#                         self.cam_error = "cap.read() failed (camera busy/disconnected)."
#                         break

#                     frame_count += 1
#                     frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)

#                     # détection visage seulement 1 frame sur N
#                     boxes = None
#                     if (frame_count % self.detect_every_n) == 0:
#                         boxes = detect_faces_haar(cascade, frame_rgb)
#                         self._last_boxes = boxes
#                         self._last_boxes_ts = time.time()
#                     else:
#                         # si pas de détection cette frame, on réutilise les dernières boxes (stabilisation)
#                         age_ms = (time.time() - self._last_boxes_ts) * 1000.0
#                         if self._last_boxes and age_ms <= self.hold_boxes_ms:
#                             boxes = self._last_boxes
#                         else:
#                             boxes = []

#                     self.frame_id += 1
#                     current_frame_id = int(self.frame_id)

#                     H, W = frame_rgb.shape[:2]
#                     face_id = 0

#                     for (x, y, w, h, conf) in boxes:
#                         face_id += 1
#                         x0 = max(0, int(x))
#                         y0 = max(0, int(y))
#                         x1 = min(W, int(x + w))
#                         y1 = min(H, int(y + h))
#                         if x1 <= x0 or y1 <= y0:
#                             continue

#                         face_rgb = frame_rgb[y0:y1, x0:x1]
#                         emo, score = infer_emotion_stub(face_rgb)

#                         cv2.rectangle(frame_bgr, (x0, y0), (x1, y1), (0, 255, 0), 2)
#                         label = f"{emo.capitalize()} ({score:.2f})"
#                         cv2.putText(
#                             frame_bgr,
#                             label,
#                             (x0, max(26, y0 - 10)),
#                             cv2.FONT_HERSHEY_SIMPLEX,
#                             0.65,
#                             (0, 255, 0),
#                             2,
#                         )

#                         if do_kafka and producer is not None:
#                             now2 = time.time()
#                             if (now2 - float(self.last_sent_ts)) >= send_interval:
#                                 self.last_sent_ts = now2
#                                 event = {
#                                     "source_id": src_id,
#                                     "frame_id": current_frame_id,
#                                     "face_id": face_id,
#                                     "producer_ts": float(now2),
#                                     "emotion": str(emo),
#                                     "score": float(score),
#                                     "bbox_x": int(x0),
#                                     "bbox_y": int(y0),
#                                     "bbox_w": int(x1 - x0),
#                                     "bbox_h": int(y1 - y0),
#                                 }
#                                 ok_send, msg = safe_send_kafka(producer, topic, event)
#                                 self.kafka_msg = msg

#                             # flush light
#                             if (now2 - float(self.last_flush_ts)) > 1.2:
#                                 self.last_flush_ts = now2
#                                 try:
#                                     producer.flush(timeout=0.2)
#                                 except Exception:
#                                     pass

#                     # resize affichage
#                     disp = frame_bgr
#                     try:
#                         h0, w0 = disp.shape[:2]
#                         if self.display_max_width and w0 > int(self.display_max_width):
#                             s = int(self.display_max_width) / float(w0)
#                             disp = cv2.resize(
#                                 disp,
#                                 (max(1, int(w0 * s)), max(1, int(h0 * s))),
#                                 interpolation=cv2.INTER_AREA,
#                             )
#                     except Exception:
#                         pass

#                     with self.lock:
#                         self.last_frame_bgr = disp

#                 if self.status != "camera_error":
#                     self.status = "stopped"

#             except Exception as e:
#                 self.status = "camera_error"
#                 self.cam_error = str(e)

#             finally:
#                 try:
#                     if producer is not None:
#                         try:
#                             producer.flush(timeout=0.5)
#                         except Exception:
#                             pass
#                         try:
#                             producer.close(timeout=1)
#                         except Exception:
#                             pass
#                 except Exception:
#                     pass

#                 try:
#                     if cap is not None:
#                         cap.release()
#                 except Exception:
#                     pass

#                 self._cap = None
#                 self._producer = None
#                 self.thread = None

#         self.thread = threading.Thread(target=_loop, daemon=True)
#         self.thread.start()

#     def stop(self):
#         # stop UI immédiat
#         self.status = "stopping"
#         self.stop_event.set()

#         with self.lock:
#             self.last_frame_bgr = None

#         # release agressif
#         try:
#             if self._cap is not None:
#                 self._cap.release()
#         except Exception:
#             pass
#         self._cap = None

#         # join court
#         try:
#             if self.thread is not None:
#                 self.thread.join(timeout=2.0)
#         except Exception:
#             pass

#         if self.thread is None or (not self.thread.is_alive()):
#             self.thread = None
#             self.status = "stopped"

class OpenCVLiveWorker:
    def __init__(self):
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

        self.last_frame_bgr = None
        self.status = "idle"
        self.cam_error = ""
        self.kafka_msg = ""

        self.frame_id = 0
        self.last_sent_ts = 0.0
        self.last_flush_ts = 0.0

        self._cap = None
        self._producer = None

        # perf / stabilité
        self.display_max_width = 960
        self.detect_every_n = 3
        self.hold_boxes_ms = 450

        self._last_boxes = []
        self._last_boxes_ts = 0.0

        self._backend_name = "DSHOW"  # DSHOW / MSMF / AUTO

        # ✅ STOP robuste (deadline + état)
        self._force_stop_deadline = 0.0
        self._force_stop_seconds = 2.0

    def is_running(self) -> bool:
        return self.thread is not None and self.thread.is_alive() and not self.stop_event.is_set()

    def _try_open_capture(self, idx: int, width: int, height: int):
        backends = []
        if self._backend_name == "DSHOW":
            backends = [cv2.CAP_DSHOW] if hasattr(cv2, "CAP_DSHOW") else [0]
        elif self._backend_name == "MSMF":
            backends = [cv2.CAP_MSMF] if hasattr(cv2, "CAP_MSMF") else [0]
        else:
            cand = []
            if hasattr(cv2, "CAP_DSHOW"):
                cand.append(cv2.CAP_DSHOW)
            if hasattr(cv2, "CAP_MSMF"):
                cand.append(cv2.CAP_MSMF)
            cand.append(0)
            backends = cand

        for be in backends:
            if self.stop_event.is_set():
                return None

            cap = cv2.VideoCapture(idx, be)

            if not cap.isOpened():
                try:
                    cap.release()
                except Exception:
                    pass
                continue

            # MJPG souvent plus fluide
            try:
                cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
            except Exception:
                pass

            # buffer minimal
            try:
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            except Exception:
                pass

            if width > 0:
                cap.set(cv2.CAP_PROP_FRAME_WIDTH, float(width))
            if height > 0:
                cap.set(cv2.CAP_PROP_FRAME_HEIGHT, float(height))

            # warmup
            ok_any = False
            for _ in range(25):
                if self.stop_event.is_set():
                    break
                ok, fr = cap.read()
                if ok and fr is not None:
                    ok_any = True
                    break
                time.sleep(0.02)

            if ok_any:
                return cap

            try:
                cap.release()
            except Exception:
                pass

        return None

    def start(
        self,
        camera_index: int,
        width: int,
        height: int,
        fps_limit: float,
        source_id: str,
        send_kafka: bool,
        min_send_interval: float,
        kafka_bootstrap: str,
        kafka_topic: str,
        backend_name: str = "DSHOW",
        detect_every_n: int = 3,
        hold_boxes_ms: int = 450,
    ):
        if self.is_running():
            return

        self.stop_event.clear()
        self.status = "starting"
        self.cam_error = ""
        self.kafka_msg = ""

        self._backend_name = backend_name
        self.detect_every_n = max(1, int(detect_every_n))
        self.hold_boxes_ms = max(0, int(hold_boxes_ms))

        self._force_stop_deadline = 0.0

        with self.lock:
            self.last_frame_bgr = None

        self._cap = None
        self._producer = None
        self._last_boxes = []
        self._last_boxes_ts = 0.0

        cascade = get_haar_face_detector()

        cam_idx = int(camera_index)
        cam_w = int(width)
        cam_h = int(height)
        fps_lim = float(fps_limit)
        src_id = str(source_id)

        do_kafka = bool(send_kafka) and KAFKA_AVAILABLE
        send_interval = float(min_send_interval)
        bootstrap = str(kafka_bootstrap)
        topic = str(kafka_topic)

        def _loop():
            cap = None
            producer = None
            try:
                cap = self._try_open_capture(cam_idx, cam_w, cam_h)
                self._cap = cap

                if cap is None:
                    self.status = "camera_error"
                    self.cam_error = (
                        f"VideoCapture open failed (index={cam_idx}). "
                        f"Try camera index 0/1/2. Close Zoom/Teams/Camera app."
                    )
                    return

                if do_kafka:
                    try:
                        producer = make_kafka_producer(bootstrap)
                        self._producer = producer
                        self.kafka_msg = f"Kafka producer OK ({bootstrap})" if producer else "Kafka producer: None"
                    except Exception as e:
                        producer = None
                        self._producer = None
                        self.kafka_msg = f"Kafka producer error: {e}"
                else:
                    producer = None
                    self._producer = None
                    self.kafka_msg = "Kafka: disabled"

                self.status = "running"
                last_frame_time = 0.0
                frame_count = 0

                while True:
                    # ✅ stop demandé -> sortie immédiate
                    if self.stop_event.is_set():
                        break

                    # limite FPS
                    now = time.time()
                    if fps_lim and fps_lim > 0:
                        min_dt = 1.0 / fps_lim
                        dt = now - last_frame_time
                        if dt < min_dt:
                            time.sleep(min_dt - dt)
                        last_frame_time = time.time()

                    # ✅ évite certains blocages: grab + retrieve
                    try:
                        grabbed = cap.grab()
                        if not grabbed:
                            # si stop demandé, on sort
                            if self.stop_event.is_set():
                                break
                            # sinon on tente encore
                            continue
                        ok, frame_bgr = cap.retrieve()
                    except Exception:
                        ok, frame_bgr = cap.read()

                    if not ok or frame_bgr is None:
                        if self.stop_event.is_set():
                            break
                        self.status = "camera_error"
                        self.cam_error = "cap.read()/retrieve failed (camera busy/disconnected)."
                        break

                    frame_count += 1
                    frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)

                    # détection visage seulement 1 frame sur N
                    if (frame_count % self.detect_every_n) == 0:
                        boxes = detect_faces_haar(cascade, frame_rgb)
                        self._last_boxes = boxes
                        self._last_boxes_ts = time.time()
                    else:
                        age_ms = (time.time() - self._last_boxes_ts) * 1000.0
                        if self._last_boxes and age_ms <= self.hold_boxes_ms:
                            boxes = self._last_boxes
                        else:
                            boxes = []

                    self.frame_id += 1
                    current_frame_id = int(self.frame_id)

                    H, W = frame_rgb.shape[:2]
                    face_id = 0

                    for (x, y, w, h, conf) in boxes:
                        face_id += 1
                        x0 = max(0, int(x))
                        y0 = max(0, int(y))
                        x1 = min(W, int(x + w))
                        y1 = min(H, int(y + h))
                        if x1 <= x0 or y1 <= y0:
                            continue

                        face_rgb = frame_rgb[y0:y1, x0:x1]
                        emo, score = infer_emotion_stub(face_rgb)

                        cv2.rectangle(frame_bgr, (x0, y0), (x1, y1), (0, 255, 0), 2)
                        label = f"{emo.capitalize()} ({score:.2f})"
                        cv2.putText(
                            frame_bgr,
                            label,
                            (x0, max(26, y0 - 10)),
                            cv2.FONT_HERSHEY_SIMPLEX,
                            0.65,
                            (0, 255, 0),
                            2,
                        )

                        if do_kafka and producer is not None:
                            now2 = time.time()
                            if (now2 - float(self.last_sent_ts)) >= send_interval:
                                self.last_sent_ts = now2
                                event = {
                                    "source_id": src_id,
                                    "frame_id": current_frame_id,
                                    "face_id": face_id,
                                    "producer_ts": float(now2),
                                    "emotion": str(emo),
                                    "score": float(score),
                                    "bbox_x": int(x0),
                                    "bbox_y": int(y0),
                                    "bbox_w": int(x1 - x0),
                                    "bbox_h": int(y1 - y0),
                                }
                                ok_send, msg = safe_send_kafka(producer, topic, event)
                                self.kafka_msg = msg

                            if (now2 - float(self.last_flush_ts)) > 1.2:
                                self.last_flush_ts = now2
                                try:
                                    producer.flush(timeout=0.2)
                                except Exception:
                                    pass

                    # resize affichage
                    disp = frame_bgr
                    try:
                        h0, w0 = disp.shape[:2]
                        if self.display_max_width and w0 > int(self.display_max_width):
                            s = int(self.display_max_width) / float(w0)
                            disp = cv2.resize(
                                disp,
                                (max(1, int(w0 * s)), max(1, int(h0 * s))),
                                interpolation=cv2.INTER_AREA,
                            )
                    except Exception:
                        pass

                    with self.lock:
                        self.last_frame_bgr = disp

                if self.status != "camera_error":
                    self.status = "stopped"

            except Exception as e:
                self.status = "camera_error"
                self.cam_error = str(e)

            finally:
                try:
                    if producer is not None:
                        try:
                            producer.flush(timeout=0.5)
                        except Exception:
                            pass
                        try:
                            producer.close(timeout=1)
                        except Exception:
                            pass
                except Exception:
                    pass

                try:
                    if cap is not None:
                        cap.release()
                except Exception:
                    pass

                self._cap = None
                self._producer = None
                self.thread = None

        self.thread = threading.Thread(target=_loop, daemon=True)
        self.thread.start()

    def stop(self):
        # ✅ stop UI immédiat + deadline
        self.status = "stopping"
        self.stop_event.set()
        self._force_stop_deadline = time.time() + float(self._force_stop_seconds)

        # on vide la dernière frame pour figer l'affichage
        with self.lock:
            self.last_frame_bgr = None

        # release agressif (souvent nécessaire sur Windows)
        try:
            if self._cap is not None:
                self._cap.release()
        except Exception:
            pass
        self._cap = None

        # join court (ne jamais bloquer l'UI)
        try:
            if self.thread is not None:
                self.thread.join(timeout=0.8)
        except Exception:
            pass

        # si le thread est toujours vivant (read() peut être bloquant sur Windows)
        if self.thread is not None and self.thread.is_alive():
            self.status = "stopping_forced"
        else:
            self.thread = None
            self.status = "stopped"

@st.cache_resource
# def get_live_worker() -> OpenCVLiveWorker:
#     return OpenCVLiveWorker()
def get_live_worker() -> OpenCVLiveWorker:
    if "live_worker" not in st.session_state:
        st.session_state["live_worker"] = OpenCVLiveWorker()
    return st.session_state["live_worker"]

# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="Emotion Dashboard", layout="wide", initial_sidebar_state="expanded")
st.markdown(APP_CSS, unsafe_allow_html=True)

lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
t = I18N[lang]
st.sidebar.header(t["sidebar_title"])

st.markdown(
    """
    <div class="app-header">
      <div class="row">
        <div class="brand">
          <div>
            <div class="brand-title">Real-Time Emotion Analytics Dashboard</div>
            <div class="brand-sub">Spark → Parquet (Analytics) · Webcam → Kafka (Live)</div>
          </div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

tab_analytics, tab_live = st.tabs([t["tab_analytics"], t["tab_live"]])

def render_topbar(title: str):
    header_subtitle = "Spark → Parquet (Analytics)  |  Webcam → Kafka (Live)"
    st.markdown(
        f"""
        <div class="topbar">
          <div class="left">
            <div class="logoWrap"><div class="logo"></div></div>
            <div>
              <div class="title">{title}</div>
              <div class="subtitle">{header_subtitle}</div>
            </div>
          </div>
          <div class="right">v2</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ============================================================
# TAB 1: Analytics
# ============================================================
def render_analytics():
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
        return

    if "event_time" in df.columns and "event_time_dt" not in df.columns:
        df["event_time_dt"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)

    for c in ["event_ts", "spark_ts", "event_time_dt"]:
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
        return

    if "score" in df.columns:
        df = df[pd.to_numeric(df["score"], errors="coerce").fillna(0) >= float(min_score)]

    source_col = "source_id" if "source_id" in df.columns else None
    if source_col:
        sources = sorted(df[source_col].dropna().unique().tolist())
        selected = st.sidebar.selectbox(t["source_filter"], ["ALL"] + sources, index=0)
        if selected != "ALL":
            df = df[df[source_col] == selected]

    if df.empty:
        st.warning(t["no_events_in_window"])
        return

    lat_p50 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 50)
    lat_p95 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 95)
    proc_p50 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 50)
    proc_p95 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 95)

    def fmt(x: float) -> str:
        return "—" if np.isnan(x) else f"{x:.0f}"

    def kpi_card(label: str, value: str, unit: str = "ms") -> str:
        unit_html = f'<span class="kpi-unit">{unit}</span>' if value != "—" and unit else ""
        return f"""
        <div class="kpi-card">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}{unit_html}</div>
        </div>
        """

    c1, c2, c3, c4, c5 = st.columns(5, gap="small")
    with c1: st.markdown(kpi_card(t["lat_p50"], fmt(lat_p50), "ms"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card(t["lat_p95"], fmt(lat_p95), "ms"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card(t["proc_p50"], fmt(proc_p50), "ms"), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card(t["proc_p95"], fmt(proc_p95), "ms"), unsafe_allow_html=True)
    with c5: st.markdown(kpi_card(t["events_rate"], f"{len(df)}", ""), unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="gray-banner">
          {t["last_updated"]}: {pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<hr class="soft">', unsafe_allow_html=True)

    left, right = st.columns([1.35, 1.0], gap="large")

    with left:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="blue-band">{t["latest_events"]}</div>', unsafe_allow_html=True)

        cols = ["event_time_dt", "event_ts", "spark_ts", "source_id", "frame_id", "emotion", "score", "e2e_latency_ms"]
        visible_cols = [c for c in cols if c in df.columns]
        df_view = df.sort_values(ts_base, ascending=False) if (ts_base and ts_base in df.columns) else df.copy()
        view = df_view[visible_cols].head(100).copy()

        # Styler peut casser selon version, donc fallback safe
        try:
            st.dataframe(style_latest_events(view), use_container_width=True, height=390)
        except Exception:
            st.dataframe(view, use_container_width=True, height=390)

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="blue-band">{t["distribution"]}</div>', unsafe_allow_html=True)

        if "emotion" in df.columns:
            emo_counts = df["emotion"].astype(str).str.lower().value_counts().reset_index()
            emo_counts.columns = ["emotion", "count"]

            fig_bar = px.bar(
                emo_counts,
                x="emotion",
                y="count",
                color="emotion",
                color_discrete_map=EMO_COLOR_MAP,
            )
            fig_bar.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=390, showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info(t["no_emotion"])

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<hr class="soft">', unsafe_allow_html=True)

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="blue-band">{t["timeline"]}</div>', unsafe_allow_html=True)

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
                counts,
                x="minute",
                y="count",
                color="emotion",
                color_discrete_map=EMO_COLOR_MAP,
            )
            fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=340)
            fig.update_xaxes(dtick=60_000)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(t["timeline_needs"])

    st.markdown("</div>", unsafe_allow_html=True)

with tab_analytics:
    render_analytics()

# ============================================================
# TAB 2: Live Webcam (Start/Stop robust)
# ============================================================
with tab_live:
    render_topbar(t["tab_live"])

    worker = get_live_worker()

    left_panel, video_panel = st.columns([0.42, 1.0], gap="large")

    # ---- LEFT PANEL (settings + status)
    with left_panel:
        st.markdown("<div class='left-panel'>", unsafe_allow_html=True)
        st.caption(t["live_help"])

        if not KAFKA_AVAILABLE:
            st.warning(t["kafka_missing"])

        default_bootstrap = os.getenv("KAFKA_BOOTSTRAP_WIN", "localhost:9092")
        default_topic = os.getenv("KAFKA_TOPIC", "test_topic")

        live_source_id = st.text_input(t["live_source_id"], value="cam_01")

        live_send_kafka = st.checkbox(
            t["live_send_kafka"],
            value=bool(KAFKA_AVAILABLE),
            disabled=not KAFKA_AVAILABLE
        )

        min_send_interval = st.number_input(
            t["live_send_interval"], min_value=0.0, max_value=5.0, value=0.5, step=0.1
        )

        kafka_bootstrap = st.text_input(t["live_kafka_bootstrap"], value=default_bootstrap)
        kafka_topic = st.text_input(t["live_topic"], value=default_topic)

        camera_index = st.number_input(t["live_camera_index"], min_value=0, max_value=10, value=0, step=1)

        cam_w = st.number_input(t["live_width"], min_value=0, max_value=3840, value=1280, step=160)
        cam_h = st.number_input(t["live_height"], min_value=0, max_value=2160, value=720, step=90)

        fps_limit = st.number_input(t["live_fps_limit"], min_value=0.0, max_value=60.0, value=12.0, step=1.0)

        
        backend_name = st.selectbox("OpenCV backend (Windows)", ["DSHOW", "MSMF", "AUTO"], index=0)
        detect_every_n = st.number_input("Detect faces every N frames", min_value=1, max_value=10, value=3, step=1)
        hold_boxes_ms = st.number_input("Hold boxes (ms)", min_value=0, max_value=2000, value=450, step=50)

        if "live_requested" not in st.session_state:
            st.session_state.live_requested = False

        # Status
        if worker.status in ("running", "starting"):
            st.markdown(f"<div class='status-ok'>✅ {t['live_status']}: {worker.status}</div>", unsafe_allow_html=True)
        elif worker.status == "camera_error":
            st.markdown(
                f"<div class='status-bad'>❌ {t['live_status']}: {t['cam_error']} — {worker.cam_error}</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(f"<div class='status-bad'>ℹ️ {t['live_status']}: {worker.status}</div>", unsafe_allow_html=True)

        st.caption(f"Kafka: {worker.kafka_msg}")

        st.markdown(f"<div class='checklist'><div class='section-title'>{t['payload_fields']}</div>", unsafe_allow_html=True)
        fields = ["source_id", "frame_id", "face_id", "producer_ts", "emotion, score", "bbox_x, bbox_y, bbox_w, bbox_h"]
        for f in fields:
            st.markdown(f"<div class='item'><span class='tick'>✓</span><span>{f}</span></div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ---- VIDEO PANEL
    with video_panel:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)

        # Buttons FIRST (so click applies before frame display)
        cbtn1, cbtn2 = st.columns(2, gap="small")
        with cbtn1:
            start_clicked = st.button(t["live_start"], use_container_width=True, key="btn_start_live_bottom")
        with cbtn2:
            stop_clicked = st.button(t["live_stop"], use_container_width=True, key="btn_stop_live_bottom")


        # if start_clicked:
        #     st.session_state.live_requested = True
        #     worker.start(
        #         camera_index=int(camera_index),
        #         width=int(cam_w),
        #         height=int(cam_h),
        #         fps_limit=float(fps_limit),
        #         source_id=str(live_source_id),
        #         send_kafka=bool(live_send_kafka),
        #         min_send_interval=float(min_send_interval),
        #         kafka_bootstrap=str(kafka_bootstrap),
        #         kafka_topic=str(kafka_topic),
        #         backend_name=str(backend_name),
        #         detect_every_n=int(detect_every_n),
        #         hold_boxes_ms=int(hold_boxes_ms),
        #     )
        #     st.rerun()

        # if stop_clicked:
        #     st.session_state.live_requested = False
        #     worker.stop()
        #     st.rerun()

        if start_clicked:
            st.session_state.live_requested = True
            worker.start(
                camera_index=int(camera_index),
                width=int(cam_w),
                height=int(cam_h),
                fps_limit=float(fps_limit),
                source_id=str(live_source_id),
                send_kafka=bool(live_send_kafka),
                min_send_interval=float(min_send_interval),
                kafka_bootstrap=str(kafka_bootstrap),
                kafka_topic=str(kafka_topic),
                backend_name=str(backend_name),
                detect_every_n=int(detect_every_n),
                hold_boxes_ms=int(hold_boxes_ms),
            )
            do_rerun()

        if stop_clicked:
            st.session_state.live_requested = False
            worker.stop()
            do_rerun()

        st.markdown("<hr class='soft'>", unsafe_allow_html=True)

        # refresh only when live is running
        # if worker.is_running() or worker.status == "starting":
        #     st_autorefresh(interval=350, key="autorefresh_live")

        if st.session_state.live_requested and (worker.is_running() or worker.status == "starting"):
            st_autorefresh(interval=450, key="autorefresh_live")  # un peu plus doux

        frame_placeholder = st.empty()

        with worker.lock:
            frame_bgr = None if worker.last_frame_bgr is None else worker.last_frame_bgr.copy()

        if frame_bgr is None:
            st.info(t["cam_not_started"])
        else:
            frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
            # ✅ Correct Streamlit API
            frame_placeholder.image(frame_rgb, channels="RGB", use_container_width=True)

        st.markdown("</div>", unsafe_allow_html=True)
        st.info(t["expected"])