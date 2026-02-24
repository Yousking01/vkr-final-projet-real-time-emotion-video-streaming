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

from pathlib import Path

APP_DIR = Path(__file__).resolve().parent        # .../src/dashboard
PROJECT_ROOT = APP_DIR.parents[2]                # .../Final_project

import cv2
import av
from streamlit_webrtc import (
    webrtc_streamer,
    VideoProcessorBase,
    WebRtcMode,
    RTCConfiguration,
)

# DuckDB (optional but recommended)
try:
    import duckdb  # pip install duckdb
    DUCKDB_AVAILABLE = True
except Exception:
    duckdb = None  # type: ignore
    DUCKDB_AVAILABLE = False

# Kafka (OPTIONAL)
try:
    from kafka import KafkaProducer  # pip install kafka-python
    KAFKA_AVAILABLE = True
except Exception:
    KafkaProducer = None  # type: ignore
    KAFKA_AVAILABLE = False

load_dotenv()

# ============================
# Thread-safe LIVE config
# ============================
LIVE_CFG_LOCK = threading.Lock()
LIVE_CFG: Dict[str, object] = {
    "source_id": "cam_01",
    "send_kafka": bool(KAFKA_AVAILABLE),
    "send_interval": 0.5,
    "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP_WIN", "localhost:9092"),
    "kafka_topic": os.getenv("KAFKA_TOPIC", "test_topic"),
    "detect_every_n": 3,
    "hold_boxes_ms": 450,
}

# ---------------------------
# Small helper: rerun
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
# I18N
# ---------------------------
I18N = {
    "en": {
        "sidebar_title": "Controls",
        "tab_analytics": "Analytics (Parquet)",
        "tab_live": "Live Webcam (WebRTC - Fast)",
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
        "live_help": "WebRTC webcam (fast) + Haar faces + emotion stub + optional Kafka throttle",
        "live_source_id": "source_id",
        "live_send_kafka": "Send events to Kafka",
        "live_send_interval": "Min seconds between sends",
        "live_kafka_bootstrap": "Kafka bootstrap (Windows)",
        "live_topic": "Kafka topic",
        "live_width": "Camera width",
        "live_height": "Camera height",
        "live_detect_every": "Detect faces every N frames",
        "live_hold_ms": "Hold boxes (ms)",
        "live_start": "Start Live",
        "live_stop": "Stop Live",
        "live_status": "Status",
        "expected": "✅ Expected: smooth video. If Kafka ON, Spark writes Parquet and it appears in Analytics.",
        "kafka_missing": "Kafka is not available (install: pip install kafka-python). Live can run without Kafka.",
    },
    "ru": {
        "sidebar_title": "Управление",
        "tab_analytics": "Аналитика (Parquet)",
        "tab_live": "Веб-камера (WebRTC - быстро)",
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
        "live_help": "WebRTC веб-камера (быстро) + Haar лица + эмоции + Kafka throttle",
        "live_source_id": "source_id",
        "live_send_kafka": "Отправлять события в Kafka",
        "live_send_interval": "Мин. секунды между отправками",
        "live_kafka_bootstrap": "Kafka bootstrap (Windows)",
        "live_topic": "Kafka topic",
        "live_width": "Ширина камеры",
        "live_height": "Высота камеры",
        "live_detect_every": "Детект каждые N кадров",
        "live_hold_ms": "Держать боксы (мс)",
        "live_start": "Старт Live",
        "live_stop": "Стоп Live",
        "live_status": "Статус",
        "expected": "✅ Ожидаемо: плавное видео. Если Kafka ON, Spark пишет Parquet и это видно в Analytics.",
        "kafka_missing": "Kafka недоступен (установи: pip install kafka-python). Live работает и без Kafka.",
    },
}

# ---------------------------
# CSS minimal (tu peux remettre ton CSS premium)
# ---------------------------
APP_CSS = r"""
<style>
.block-container { max-width: none !important; padding-top: 0.70rem; padding-left: 2.2rem; padding-right: 2.2rem; }
#MainMenu {visibility: hidden;} footer {visibility: hidden;}
</style>
"""

# ---------------------------
# Emotion stub
# ---------------------------
EMO_LIST = ["happy", "sad", "angry", "neutral", "surprised", "fear"]

EMO_COLOR_MAP: Dict[str, str] = {
    "happy": "#1f77b4",
    "sad": "#d62728",
    "angry": "#ff7f0e",
    "neutral": "#7f7f7f",
    "surprised": "#9467bd",
    "fear": "#bcbd22",
}

def infer_emotion_stub(face_rgb: np.ndarray) -> Tuple[str, float]:
    label = EMO_LIST[int(time.time()) % len(EMO_LIST)]
    score = 0.70 + 0.20 * ((int(time.time() * 10) % 5) / 5)
    return label, float(min(score, 0.95))

# ---------------------------
# Haar face detector
# ---------------------------
@st.cache_resource
def get_haar_face_detector():
    cascade_path = os.path.join(cv2.data.haarcascades, "haarcascade_frontalface_default.xml")
    cascade = cv2.CascadeClassifier(cascade_path)
    if cascade.empty():
        raise RuntimeError(f"Failed to load Haar cascade: {cascade_path}")
    return cascade

def detect_faces_haar_fast(cascade, frame_rgb: np.ndarray) -> List[Tuple[int, int, int, int, float]]:
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

# ---------------------------
# Kafka helpers
# ---------------------------
def make_kafka_producer(bootstrap: str):
    if not KAFKA_AVAILABLE or not bootstrap:
        return None
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=2,
        acks=1,
        linger_ms=10,
    )

# ---------------------------
# Parquet loading (DuckDB accelerated)
# ---------------------------
def _normalize_path_for_duckdb(path: str) -> str:
    return path.replace("\\", "/")

# @st.cache_data(ttl=2)
# def load_parquet_folder(path: str) -> pd.DataFrame:
#     if not path or not os.path.exists(path):
#         return pd.DataFrame()

#     if DUCKDB_AVAILABLE:
#         try:
#             p = _normalize_path_for_duckdb(path)
#             con = duckdb.connect(database=":memory:", read_only=False)
#             df = con.execute(
#                 f"SELECT * FROM read_parquet('{p}', hive_partitioning=true, union_by_name=true)"
#             ).df()
#             con.close()
#             return df
#         except Exception:
#             pass

#     # fallback
#     try:
#         import pyarrow.dataset as ds
#         dataset = ds.dataset(path, format="parquet", partitioning="hive")
#         table = dataset.to_table()
#         return table.to_pandas()
#     except Exception:
#         try:
#             return pd.read_parquet(path, engine="pyarrow")
#         except Exception:
#             return pd.DataFrame()

@st.cache_data(ttl=2)
def load_parquet_folder(path: str) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()

    p = Path(path)

    # si relatif + introuvable -> on le recale sur la racine du projet
    if not p.is_absolute() and not p.exists():
        p = (PROJECT_ROOT / p).resolve()

    if not p.exists():
        return pd.DataFrame()

    # DuckDB (si dispo)
    if DUCKDB_AVAILABLE:
        try:
            pp = str(p).replace("\\", "/")
            globp = pp.rstrip("/") + "/**/*.parquet"
            con = duckdb.connect(database=":memory:")
            df = con.execute(
                f"SELECT * FROM read_parquet('{globp}', hive_partitioning=true, union_by_name=true)"
            ).df()
            con.close()
            return df
        except Exception:
            pass

    # fallback pyarrow
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
    candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time"]
    for c in candidates:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
            if not df[c].isna().all():
                return c
    return None

# ============================
# WebRTC Processor
# ============================
class FaceKafkaWebRTCProcessor(VideoProcessorBase):
    def __init__(self):
        self.cascade = get_haar_face_detector()
        self.frame_id = 0
        self.last_sent_ts = 0.0
        self.last_flush_ts = 0.0

        self._producer = None
        self._producer_bootstrap = ""
        self._producer_topic = ""

        self._last_boxes: List[Tuple[int, int, int, int, float]] = []
        self._last_boxes_ts = 0.0

    def _ensure_producer(self, enabled: bool, bootstrap: str, topic: str):
        if not enabled or not KAFKA_AVAILABLE:
            if self._producer is not None:
                try:
                    self._producer.flush(timeout=0.2)
                except Exception:
                    pass
                try:
                    self._producer.close(timeout=1)
                except Exception:
                    pass
            self._producer = None
            self._producer_bootstrap = ""
            self._producer_topic = ""
            return

        if self._producer is None or self._producer_bootstrap != bootstrap or self._producer_topic != topic:
            try:
                self._producer = make_kafka_producer(bootstrap)
                self._producer_bootstrap = bootstrap
                self._producer_topic = topic
            except Exception:
                self._producer = None
                self._producer_bootstrap = ""
                self._producer_topic = ""

    def recv(self, frame: av.VideoFrame) -> av.VideoFrame:
        img_bgr = frame.to_ndarray(format="bgr24")

        with LIVE_CFG_LOCK:
            cfg = dict(LIVE_CFG)

        source_id = str(cfg.get("source_id", "cam_01"))
        send_kafka = bool(cfg.get("send_kafka", False))
        send_interval = float(cfg.get("send_interval", 0.5))
        kafka_bootstrap = str(cfg.get("kafka_bootstrap", "localhost:9092"))
        kafka_topic = str(cfg.get("kafka_topic", "test_topic"))
        detect_every_n = int(cfg.get("detect_every_n", 3))
        hold_boxes_ms = int(cfg.get("hold_boxes_ms", 450))

        self._ensure_producer(send_kafka, kafka_bootstrap, kafka_topic)

        self.frame_id += 1
        frame_count = self.frame_id

        img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)

        if (frame_count % max(1, detect_every_n)) == 0:
            boxes = detect_faces_haar_fast(self.cascade, img_rgb)
            self._last_boxes = boxes
            self._last_boxes_ts = time.time()
        else:
            age_ms = (time.time() - self._last_boxes_ts) * 1000.0
            if self._last_boxes and age_ms <= max(0, hold_boxes_ms):
                boxes = self._last_boxes
            else:
                boxes = []

        H, W = img_rgb.shape[:2]
        now = time.time()

        face_id = 0
        for (x, y, w, h, conf) in boxes:
            face_id += 1
            x0 = max(0, int(x))
            y0 = max(0, int(y))
            x1 = min(W, int(x + w))
            y1 = min(H, int(y + h))
            if x1 <= x0 or y1 <= y0:
                continue

            face_rgb = img_rgb[y0:y1, x0:x1]
            emo, score = infer_emotion_stub(face_rgb)

            cv2.rectangle(img_bgr, (x0, y0), (x1, y1), (0, 255, 0), 2)
            cv2.putText(
                img_bgr,
                f"{emo.capitalize()} ({score:.2f})",
                (x0, max(26, y0 - 10)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.65,
                (0, 255, 0),
                2,
            )

            if self._producer is not None and (now - self.last_sent_ts) >= send_interval:
                self.last_sent_ts = now
                event = {
                    "source_id": source_id,
                    "frame_id": int(frame_count),
                    "face_id": int(face_id),
                    "producer_ts": float(now),
                    "emotion": str(emo),
                    "score": float(score),
                    "bbox_x": int(x0),
                    "bbox_y": int(y0),
                    "bbox_w": int(x1 - x0),
                    "bbox_h": int(y1 - y0),
                }
                try:
                    self._producer.send(kafka_topic, event)
                except Exception:
                    pass

                if (now - float(self.last_flush_ts)) > 1.2:
                    self.last_flush_ts = now
                    try:
                        self._producer.flush(timeout=0.2)
                    except Exception:
                        pass

        return av.VideoFrame.from_ndarray(img_bgr, format="bgr24")

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
          <div style="font-size:12px; opacity:0.85; font-weight:700;">
            Spark → Parquet (Analytics)  |  Webcam → Kafka (Live)
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ============================================================
# TAB 1: Analytics (NO FRAGMENTS)
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
        # timestamps
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
                    cols = ["event_time_dt", "event_ts", "spark_ts", "source_id", "frame_id", "emotion", "score", "e2e_latency_ms"]
                    visible_cols = [c for c in cols if c in df.columns]
                    df_view = df.sort_values(ts_base, ascending=False) if (ts_base and ts_base in df.columns) else df.copy()
                    st.dataframe(df_view[visible_cols].head(100), use_container_width=True, height=380)

                with right:
                    st.subheader(t["distribution"])
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
                            counts,
                            x="minute",
                            y="count",
                            color="emotion",
                            color_discrete_map=EMO_COLOR_MAP,
                        )
                        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=320)
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(t["timeline_needs"])

# ============================================================
# TAB 2: Live (WebRTC)
# ============================================================
with tab_live:
    render_topbar(t["tab_live"])

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

        kafka_bootstrap = st.text_input(
            t["live_kafka_bootstrap"],
            value=str(LIVE_CFG.get("kafka_bootstrap", "localhost:9092"))
        )

        kafka_topic = st.text_input(
            t["live_topic"],
            value=str(LIVE_CFG.get("kafka_topic", "test_topic"))
        )

        cam_w = st.number_input(t["live_width"], min_value=320, max_value=1920, value=640, step=160)
        cam_h = st.number_input(t["live_height"], min_value=240, max_value=1080, value=480, step=120)

        detect_every_n = st.number_input(t["live_detect_every"], min_value=1, max_value=10, value=3, step=1)
        hold_boxes_ms = st.number_input(t["live_hold_ms"], min_value=0, max_value=2000, value=450, step=50)

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

        # Update live config for processor
        with LIVE_CFG_LOCK:
            LIVE_CFG.update({
                "source_id": live_source_id,
                "send_kafka": bool(live_send_kafka),
                "send_interval": float(min_send_interval),
                "kafka_bootstrap": str(kafka_bootstrap),
                "kafka_topic": str(kafka_topic),
                "detect_every_n": int(detect_every_n),
                "hold_boxes_ms": int(hold_boxes_ms),
            })

    with video_panel:
        if st.session_state.live_on:
            rtc_configuration = RTCConfiguration(
                {"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]}
            )

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
                async_processing=True,
            )
            st.info(t["expected"])
        else:
            st.info("Live is stopped. Click Start Live.")