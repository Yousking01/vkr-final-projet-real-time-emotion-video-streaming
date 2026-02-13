import os
import pandas as pd
import numpy as np
import streamlit as st

# ---------------------------
# 1) Language dictionary (EN default)
# ---------------------------
I18N = {
    "en": {
        "app_title": "Real-Time Emotion Analytics Dashboard",
        "sidebar_title": "Controls",
        "language": "Language",
        "data_path": "Parquet folder",
        "refresh": "Refresh",
        "auto_refresh": "Auto refresh (seconds)",
        "source_filter": "Source (camera)",
        "time_filter": "Time filter (last N minutes)",
        "min_score": "Min score",
        "kpis": "Key metrics",
        "lat_p50": "E2E latency p50 (ms)",
        "lat_p95": "E2E latency p95 (ms)",
        "proc_p50": "Processing p50 (ms)",
        "proc_p95": "Processing p95 (ms)",
        "events_rate": "Events (rows)",
        "latest_events": "Latest events",
        "distribution": "Emotion distribution",
        "timeline": "Emotion timeline (counts per minute)",
        "no_data": "No data found in the selected folder.",
        "note": "Tip: keep Spark job running to see live updates.",
        "no_ts_found": "No usable timestamp column found (or all values empty). Showing rows without time filter/sorting.",
        "no_emotion": "No emotion column found.",
        "timeline_needs": "Timeline needs a timestamp column + emotion column.",
    },
    "ru": {
        "app_title": "Панель аналитики эмоций в реальном времени",
        "sidebar_title": "Управление",
        "language": "Язык",
        "data_path": "Папка Parquet",
        "refresh": "Обновить",
        "auto_refresh": "Автообновление (сек.)",
        "source_filter": "Источник (камера)",
        "time_filter": "Фильтр времени (последние N минут)",
        "min_score": "Мин. уверенность (score)",
        "kpis": "Ключевые метрики",
        "lat_p50": "Задержка E2E p50 (мс)",
        "lat_p95": "Задержка E2E p95 (мс)",
        "proc_p50": "Время обработки p50 (мс)",
        "proc_p95": "Время обработки p95 (мс)",
        "events_rate": "События (строки)",
        "latest_events": "Последние события",
        "distribution": "Распределение эмоций",
        "timeline": "Динамика эмоций (кол-во в минуту)",
        "no_data": "В выбранной папке нет данных.",
        "note": "Совет: оставь Spark job запущенным, чтобы видеть обновления в реальном времени.",
        "no_ts_found": "Не найдена подходящая колонка времени (или все значения пустые). Показываю данные без фильтра/сортировки по времени.",
        "no_emotion": "Колонка emotion не найдена.",
        "timeline_needs": "Для таймлайна нужны колонка времени и колонка emotion.",
    },
}

# ---------------------------
# 2) Helpers
# ---------------------------
@st.cache_data(ttl=2)
def load_parquet_folder(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()

    try:
        df = pd.read_parquet(path)
    except Exception:
        import pyarrow.dataset as ds
        dataset = ds.dataset(path, format="parquet")
        df = dataset.to_table().to_pandas()

    return df


def percentile_safe(series: pd.Series, q: float):
    series = pd.to_numeric(series, errors="coerce").dropna()
    if len(series) == 0:
        return np.nan
    return float(np.percentile(series, q))


def ensure_datetime(df: pd.DataFrame, colname: str) -> None:
    """Convert df[colname] to datetime safely in-place."""
    if colname in df.columns:
        df[colname] = pd.to_datetime(df[colname], errors="coerce")


def pick_timestamp_column(df: pd.DataFrame):
    """
    Pick the best available timestamp column.
    Supports both old/new naming:
    event_ts, spark_ts, event_time, etc.
    Returns chosen column name OR None.
    """
    # Candidates in priority order
    candidates = [
        "event_ts",
        "spark_ts",
        "event_time",      # your parquet_events_v2 currently has this
        "event_time_dt",   # optional derived
    ]

    for c in candidates:
        if c in df.columns:
            # if exists but all null -> not usable
            if not df[c].isna().all():
                return c
    return None


# ---------------------------
# 3) UI
# ---------------------------
st.set_page_config(page_title="Emotion Dashboard", layout="wide")

lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
t = I18N[lang]

st.title(t["app_title"])
st.sidebar.header(t["sidebar_title"])

default_path = "data/parquet_events_v2"
parquet_path = st.sidebar.text_input(t["data_path"], value=default_path)

min_minutes = st.sidebar.number_input(t["time_filter"], min_value=1, max_value=1440, value=60, step=5)
min_score = st.sidebar.slider(t["min_score"], min_value=0.0, max_value=1.0, value=0.0, step=0.05)

refresh_now = st.sidebar.button(t["refresh"])
auto_refresh_s = st.sidebar.number_input(t["auto_refresh"], min_value=0, max_value=60, value=0, step=1)
st.sidebar.caption(t["note"])

if auto_refresh_s and auto_refresh_s > 0:
    st.autorefresh(interval=int(auto_refresh_s) * 1000, key="auto_refresh_key")

df = load_parquet_folder(parquet_path)

if df.empty:
    st.warning(t["no_data"])
    st.stop()

# ---------------------------
# 4) Timestamps: normalize
# ---------------------------
# Convert known time columns to datetime if present
ensure_datetime(df, "event_ts")
ensure_datetime(df, "spark_ts")
ensure_datetime(df, "event_time")

# If event_time exists but may be string/None, create event_time_dt for safer handling
if "event_time" in df.columns:
    df["event_time_dt"] = pd.to_datetime(df["event_time"], errors="coerce")

ts_base = pick_timestamp_column(df)

# Filter by time window if possible
if ts_base is not None:
    now_ts = df[ts_base].max()
    if pd.notna(now_ts):
        cutoff = now_ts - pd.Timedelta(minutes=int(min_minutes))
        df = df[df[ts_base] >= cutoff]
else:
    st.info(t["no_ts_found"])

# Filter by score
if "score" in df.columns:
    df = df[pd.to_numeric(df["score"], errors="coerce").fillna(0) >= float(min_score)]

# Source filter
source_col = "source_id" if "source_id" in df.columns else None
if source_col:
    sources = sorted([s for s in df[source_col].dropna().unique().tolist()])
    selected = st.sidebar.selectbox(t["source_filter"], ["ALL"] + sources, index=0)
    if selected != "ALL":
        df = df[df[source_col] == selected]

# If after filters df empty
if df.empty:
    st.warning(t["no_data"])
    st.stop()

# ---------------------------
# 5) KPIs
# ---------------------------
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

lat_p50 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 50)
lat_p95 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 95)

proc_p50 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 50)
proc_p95 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 95)

kpi1.metric(t["lat_p50"], f"{lat_p50:.1f}" if not np.isnan(lat_p50) else "—")
kpi2.metric(t["lat_p95"], f"{lat_p95:.1f}" if not np.isnan(lat_p95) else "—")
kpi3.metric(t["proc_p50"], f"{proc_p50:.1f}" if not np.isnan(proc_p50) else "—")
kpi4.metric(t["proc_p95"], f"{proc_p95:.1f}" if not np.isnan(proc_p95) else "—")
kpi5.metric(t["events_rate"], f"{len(df)}")

st.divider()

# ---------------------------
# 6) Main charts + table
# ---------------------------
left, right = st.columns([1.2, 1])

with left:
    st.subheader(t["latest_events"])

    # Prefer showing whichever timestamp column exists
    # We'll include multiple options; only those present are shown
    cols = [
        "event_ts", "spark_ts", "event_time_dt", "event_time",
        "source_id", "frame_id", "face_id",
        "emotion", "score",
        "bbox_x", "bbox_y", "bbox_w", "bbox_h",
        "processing_time_ms", "e2e_latency_ms"
    ]
    visible_cols = [c for c in cols if c in df.columns]

    # Safe sorting
    if ts_base is not None and ts_base in df.columns and not df[ts_base].isna().all():
        df_view = df.sort_values(ts_base, ascending=False)
    else:
        df_view = df

    st.dataframe(df_view[visible_cols].head(200), use_container_width=True)

with right:
    st.subheader(t["distribution"])
    if "emotion" in df.columns:
        emo_counts = df["emotion"].astype(str).value_counts().reset_index()
        emo_counts.columns = ["emotion", "count"]
        st.bar_chart(emo_counts.set_index("emotion"))
    else:
        st.info(t["no_emotion"])

st.divider()

# Timeline
st.subheader(t["timeline"])
if ts_base is not None and "emotion" in df.columns:
    tmp = df[[ts_base, "emotion"]].dropna().copy()

    # If timestamp is not datetime (should be), try coercion once
    tmp[ts_base] = pd.to_datetime(tmp[ts_base], errors="coerce")
    tmp = tmp.dropna(subset=[ts_base])

    if tmp.empty:
        st.info(t["timeline_needs"])
    else:
        tmp["minute"] = tmp[ts_base].dt.floor("min")
        timeline = tmp.groupby(["minute", "emotion"]).size().reset_index(name="count")
        pivot = timeline.pivot(index="minute", columns="emotion", values="count").fillna(0).sort_index()
        st.line_chart(pivot)
else:
    st.info(t["timeline_needs"])
