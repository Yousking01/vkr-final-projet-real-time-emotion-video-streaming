# import os
# import pandas as pd
# import numpy as np
# import streamlit as st

# # ---------------------------
# # 1) Language dictionary (EN default)
# # ---------------------------
# I18N = {
#     "en": {
#         "app_title": "Real-Time Emotion Analytics Dashboard",
#         "sidebar_title": "Controls",
#         "language": "Language",
#         "data_path": "Parquet folder",
#         "refresh": "Refresh",
#         "auto_refresh": "Auto refresh (seconds)",
#         "source_filter": "Source (camera)",
#         "time_filter": "Time filter (last N minutes)",
#         "min_score": "Min score",
#         "kpis": "Key metrics",
#         "lat_p50": "E2E latency p50 (ms)",
#         "lat_p95": "E2E latency p95 (ms)",
#         "proc_p50": "Processing p50 (ms)",
#         "proc_p95": "Processing p95 (ms)",
#         "events_rate": "Events (rows)",
#         "latest_events": "Latest events",
#         "distribution": "Emotion distribution",
#         "timeline": "Emotion timeline (counts per minute)",
#         "no_data": "No data found in the selected folder.",
#         "note": "Tip: keep Spark job running to see live updates.",
#         "no_ts_found": "No usable timestamp column found (or all values empty). Showing rows without time filter/sorting.",
#         "no_emotion": "No emotion column found.",
#         "timeline_needs": "Timeline needs a timestamp column + emotion column.",
#     },
#     "ru": {
#         "app_title": "Панель аналитики эмоций в реальном времени",
#         "sidebar_title": "Управление",
#         "language": "Язык",
#         "data_path": "Папка Parquet",
#         "refresh": "Обновить",
#         "auto_refresh": "Автообновление (сек.)",
#         "source_filter": "Источник (камера)",
#         "time_filter": "Фильтр времени (последние N минут)",
#         "min_score": "Мин. уверенность (score)",
#         "kpis": "Ключевые метрики",
#         "lat_p50": "Задержка E2E p50 (мс)",
#         "lat_p95": "Задержка E2E p95 (мс)",
#         "proc_p50": "Время обработки p50 (мс)",
#         "proc_p95": "Время обработки p95 (мс)",
#         "events_rate": "События (строки)",
#         "latest_events": "Последние события",
#         "distribution": "Распределение эмоций",
#         "timeline": "Динамика эмоций (кол-во в минуту)",
#         "no_data": "В выбранной папке нет данных.",
#         "note": "Совет: оставь Spark job запущенным, чтобы видеть обновления в реальном времени.",
#         "no_ts_found": "Не найдена подходящая колонка времени (или все значения пустые). Показываю данные без фильтра/сортировки по времени.",
#         "no_emotion": "Колонка emotion не найдена.",
#         "timeline_needs": "Для таймлайна нужны колонка времени и колонка emotion.",
#     },
# }

# # ---------------------------
# # 2) Helpers
# # ---------------------------
# @st.cache_data(ttl=2)
# def load_parquet_folder(path: str) -> pd.DataFrame:
#     """
#     Load a parquet folder (Spark output directory) as a pandas DataFrame.
#     Works on Windows for a directory path like: data/parquet_events_v2
#     """
#     if not path or not os.path.exists(path):
#         return pd.DataFrame()

#     # pandas can read parquet *directory* (dataset) if engine supports it
#     try:
#         df = pd.read_parquet(path)
#         return df
#     except Exception:
#         # fallback: use pyarrow.dataset
#         try:
#             import pyarrow.dataset as ds
#             dataset = ds.dataset(path, format="parquet")
#             return dataset.to_table().to_pandas()
#         except Exception:
#             return pd.DataFrame()


# def percentile_safe(series: pd.Series, q: float) -> float:
#     series = pd.to_numeric(series, errors="coerce").dropna()
#     if len(series) == 0:
#         return np.nan
#     return float(np.percentile(series, q))


# def ensure_datetime(df: pd.DataFrame, colname: str) -> None:
#     """Convert df[colname] to datetime safely in-place."""
#     if colname in df.columns:
#         df[colname] = pd.to_datetime(df[colname], errors="coerce", utc=False)


# def pick_timestamp_column(df: pd.DataFrame):
#     """
#     Pick the best available timestamp column, robustly.
#     Priority: event_ts > spark_ts > event_time_dt > event_time
#     Returns chosen column name OR None.
#     """
#     candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time"]

#     for c in candidates:
#         if c in df.columns:
#             # force datetime coercion so we don't pick a bad string column
#             df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)
#             if not df[c].isna().all():
#                 return c
#     return None


# # ---------------------------
# # 3) UI
# # ---------------------------
# st.set_page_config(page_title="Emotion Dashboard", layout="wide")

# # Sidebar controls
# lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
# t = I18N[lang]

# st.title(t["app_title"])
# st.sidebar.header(t["sidebar_title"])

# default_path = "data/parquet_events_v2"
# parquet_path = st.sidebar.text_input(t["data_path"], value=default_path)

# min_minutes = st.sidebar.number_input(
#     t["time_filter"], min_value=1, max_value=1440, value=60, step=5
# )
# min_score = st.sidebar.slider(
#     t["min_score"], min_value=0.0, max_value=1.0, value=0.0, step=0.05
# )

# refresh_now = st.sidebar.button(t["refresh"])
# auto_refresh_s = st.sidebar.number_input(
#     t["auto_refresh"], min_value=0, max_value=60, value=0, step=1
# )
# st.sidebar.caption(t["note"])

# # Auto refresh
# if auto_refresh_s and int(auto_refresh_s) > 0:
#     st.autorefresh(interval=int(auto_refresh_s) * 1000, key="auto_refresh_key")

# # Load data
# df = load_parquet_folder(parquet_path)

# if df.empty:
#     st.warning(t["no_data"])
#     st.stop()

# # ---- Timestamp normalization (do it early, before filtering/sorting) ----
# ensure_datetime(df, "event_ts")
# ensure_datetime(df, "spark_ts")
# ensure_datetime(df, "event_time")

# # Safer derived column for event_time (sometimes string/None)
# if "event_time" in df.columns:
#     df["event_time_dt"] = pd.to_datetime(df["event_time"], errors="coerce", utc=False)

# ts_base = pick_timestamp_column(df)

# # If no timestamp exists, we still continue but warn
# if ts_base is None:
#     # Make sure your I18N has this key; otherwise replace with st.info("...")
#     st.info(t.get("no_ts_found", "No usable timestamp column found (event_ts/spark_ts/event_time)."))

# # ---------------------------
# # 4) Timestamps: normalize
# # ---------------------------

# # 1) Convert known time columns to datetime if present
# ensure_datetime(df, "event_ts")
# ensure_datetime(df, "spark_ts")
# ensure_datetime(df, "event_time")

# # 2) If event_time exists, create event_time_dt (safe column for display)
# if "event_time" in df.columns:
#     df["event_time_dt"] = pd.to_datetime(df["event_time"], errors="coerce")

# # 3) Pick base timestamp column
# ts_base = pick_timestamp_column(df)

# # 4) If ts_base exists but is all NaT, fallback properly
# if ts_base is not None:
#     # Ensure it's datetime again (in case pick_timestamp_column returns something unexpected)
#     df[ts_base] = pd.to_datetime(df[ts_base], errors="coerce")
#     if df[ts_base].isna().all():
#         ts_base = None

# # 5) Filter by time window if possible
# if ts_base is not None:
#     now_ts = df[ts_base].max()
#     if pd.notna(now_ts):
#         cutoff = now_ts - pd.Timedelta(minutes=int(min_minutes))
#         df = df[df[ts_base] >= cutoff]
# else:
#     st.info(t["no_ts_found"])

# # Filter by score
# if "score" in df.columns:
#     df = df[pd.to_numeric(df["score"], errors="coerce").fillna(0) >= float(min_score)]

# # Source filter
# source_col = "source_id" if "source_id" in df.columns else None
# if source_col:
#     sources = sorted(df[source_col].dropna().unique().tolist())
#     selected = st.sidebar.selectbox(t["source_filter"], ["ALL"] + sources, index=0)
#     if selected != "ALL":
#         df = df[df[source_col] == selected]

# # If after filters df empty
# if df.empty:
#     st.warning(t["no_data"])
#     st.stop()

# # ---------------------------
# # 5) KPIs
# # ---------------------------
# kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

# lat_p50 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 50)
# lat_p95 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 95)
# proc_p50 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 50)
# proc_p95 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 95)

# kpi1.metric(t["lat_p50"], f"{lat_p50:.1f}" if not np.isnan(lat_p50) else "—")
# kpi2.metric(t["lat_p95"], f"{lat_p95:.1f}" if not np.isnan(lat_p95) else "—")
# kpi3.metric(t["proc_p50"], f"{proc_p50:.1f}" if not np.isnan(proc_p50) else "—")
# kpi4.metric(t["proc_p95"], f"{proc_p95:.1f}" if not np.isnan(proc_p95) else "—")
# kpi5.metric(t["events_rate"], f"{len(df)}")

# st.divider()

# # ---------------------------
# # 6) Main charts + table
# # ---------------------------
# left, right = st.columns([1.2, 1])

# with left:
#     st.subheader(t["latest_events"])

#     cols = [
#         "event_ts", "spark_ts", "event_time_dt", "event_time",
#         "source_id", "frame_id", "face_id",
#         "emotion", "score",
#         "bbox_x", "bbox_y", "bbox_w", "bbox_h",
#         "processing_time_ms", "e2e_latency_ms"
#     ]
#     visible_cols = [c for c in cols if c in df.columns]

#     # Safe sorting: if no usable ts_base => just show as-is
#     if ts_base is not None and ts_base in df.columns and not df[ts_base].isna().all():
#         df_view = df.sort_values(ts_base, ascending=False)
#     else:
#         df_view = df

#     st.dataframe(df_view[visible_cols].head(200), use_container_width=True)

# with right:
#     st.subheader(t["distribution"])
#     if "emotion" in df.columns:
#         emo_counts = df["emotion"].astype(str).value_counts().reset_index()
#         emo_counts.columns = ["emotion", "count"]
#         st.bar_chart(emo_counts.set_index("emotion"))
#     else:
#         st.info(t["no_emotion"])

# st.divider()

# # ---------------------------
# # 7) Timeline
# # ---------------------------
# st.subheader(t["timeline"])

# # For timeline, prefer event_ts -> spark_ts -> event_time_dt -> event_time
# timeline_ts = None
# for c in ["event_ts", "spark_ts", "event_time_dt", "event_time"]:
#     if c in df.columns:
#         timeline_ts = c
#         break

# if timeline_ts is not None and "emotion" in df.columns:
#     tmp = df[[timeline_ts, "emotion"]].copy()
#     tmp[timeline_ts] = pd.to_datetime(tmp[timeline_ts], errors="coerce")
#     tmp = tmp.dropna(subset=[timeline_ts])

#     if tmp.empty:
#         st.info(t["timeline_needs"])
#     else:
#         tmp["minute"] = tmp[timeline_ts].dt.floor("min")
#         timeline = tmp.groupby(["minute", "emotion"]).size().reset_index(name="count")
#         pivot = timeline.pivot(index="minute", columns="emotion", values="count").fillna(0).sort_index()
#         st.line_chart(pivot)
# else:
#     st.info(t["timeline_needs"])


# import os
# import pandas as pd
# import numpy as np
# import streamlit as st

# from streamlit_autorefresh import st_autorefresh
# import plotly.express as px

# from dotenv import load_dotenv
# load_dotenv()

# # ---------------------------
# # 1) Language dictionary (EN default)
# # ---------------------------
# I18N = {
#     "en": {
#         "app_title": "Real-Time Emotion Analytics Dashboard",
#         "sidebar_title": "Controls",
#         "language": "Language",
#         "data_path": "Parquet folder",
#         "refresh": "Refresh",
#         "auto_refresh": "Auto refresh (seconds)",
#         "source_filter": "Source (camera)",
#         "time_filter": "Time filter (last N minutes)",
#         "min_score": "Min score",
#         "kpis": "Key metrics",
#         "lat_p50": "E2E latency p50 (ms)",
#         "lat_p95": "E2E latency p95 (ms)",
#         "proc_p50": "Processing p50 (ms)",
#         "proc_p95": "Processing p95 (ms)",
#         "events_rate": "Events (rows)",
#         "latest_events": "Latest events",
#         "distribution": "Emotion distribution",
#         "timeline": "Emotion timeline (counts per minute)",
#         "no_data": "No data found in the selected folder.",
#         "note": "Tip: keep Spark job running to see live updates.",
#         "no_ts_found": "No usable timestamp column found (or all values empty). Showing rows without time filter/sorting.",
#         "no_emotion": "No emotion column found.",
#         "timeline_needs": "Timeline needs a timestamp column + emotion column.",
#         "no_events_in_window": "No events in the selected time window. Send messages to Kafka or increase the time window.",
#         "last_updated": "Last updated",
#     },
#     "ru": {
#         "app_title": "Панель аналитики эмоций в реальном времени",
#         "sidebar_title": "Управление",
#         "language": "Язык",
#         "data_path": "Папка Parquet",
#         "refresh": "Обновить",
#         "auto_refresh": "Автообновление (сек.)",
#         "source_filter": "Источник (камера)",
#         "time_filter": "Фильтр времени (последние N минут)",
#         "min_score": "Мин. уверенность (score)",
#         "kpis": "Ключевые метрики",
#         "lat_p50": "Задержка E2E p50 (мс)",
#         "lat_p95": "Задержка E2E p95 (мс)",
#         "proc_p50": "Время обработки p50 (мс)",
#         "proc_p95": "Время обработки p95 (мс)",
#         "events_rate": "События (строки)",
#         "latest_events": "Последние события",
#         "distribution": "Распределение эмоций",
#         "timeline": "Динамика эмоций (кол-во в минуту)",
#         "no_data": "В выбранной папке нет данных.",
#         "note": "Совет: оставь Spark job запущенным, чтобы видеть обновления в реальном времени.",
#         "no_ts_found": "Не найдена подходящая колонка времени (или все значения пустые). Показываю данные без фильтра/сортировки по времени.",
#         "no_emotion": "Колонка emotion не найдена.",
#         "timeline_needs": "Для таймлайна нужны колонка времени и колонка emotion.",
#         "no_events_in_window": "В выбранном окне времени нет событий. Отправь сообщения в Kafka или увеличь окно времени.",
#         "last_updated": "Последнее обновление",
#     },
# }

# # ---------------------------
# # 2) Helpers
# # ---------------------------
# @st.cache_data(ttl=2)
# def load_parquet_folder(path: str) -> pd.DataFrame:
#     """
#     Load a parquet folder (Spark output directory) as a pandas DataFrame.
#     Works on Windows for a directory path like: data/parquet_events_v2
#     """
#     if not path or not os.path.exists(path):
#         return pd.DataFrame()

#     # pandas can read parquet *directory* (dataset) if engine supports it
#     try:
#         df = pd.read_parquet(path)
#         return df
#     except Exception:
#         # fallback: use pyarrow.dataset
#         try:
#             import pyarrow.dataset as ds
#             dataset = ds.dataset(path, format="parquet")
#             return dataset.to_table().to_pandas()
#         except Exception:
#             return pd.DataFrame()


# def percentile_safe(series: pd.Series, q: float) -> float:
#     s = pd.to_numeric(series, errors="coerce").dropna()
#     if len(s) == 0:
#         return np.nan
#     return float(np.percentile(s, q))


# def pick_timestamp_column(df: pd.DataFrame):
#     """
#     Pick the best available timestamp column, robustly.
#     Priority: event_ts > spark_ts > event_time_dt > event_time
#     Returns chosen column name OR None.
#     """
#     candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time"]
#     for c in candidates:
#         if c in df.columns:
#             df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)
#             if not df[c].isna().all():
#                 return c
#     return None


# # ---------------------------
# # 3) UI
# # ---------------------------
# st.set_page_config(page_title="Emotion Dashboard", layout="wide")

# # Sidebar controls
# lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
# t = I18N[lang]

# st.title(t["app_title"])
# st.sidebar.header(t["sidebar_title"])

# # default_path = "data/parquet_events_v2"
# default_path = os.getenv("PARQUET_PATH", "data/parquet_events_v2")
# parquet_path = st.sidebar.text_input(t["data_path"], value=default_path)

# min_minutes = st.sidebar.number_input(
#     t["time_filter"], min_value=1, max_value=1440, value=60, step=5
# )
# min_score = st.sidebar.slider(
#     t["min_score"], min_value=0.0, max_value=1.0, value=0.0, step=0.05
# )

# refresh_now = st.sidebar.button(t["refresh"])
# auto_refresh_s = st.sidebar.number_input(
#     t["auto_refresh"], min_value=0, max_value=3600, value=10, step=1
# )
# st.sidebar.caption(t["note"])

# # Force reload when user clicks Refresh
# if refresh_now:
#     st.cache_data.clear()

# # Auto refresh (PRO)
# if int(auto_refresh_s) > 0:
#     st_autorefresh(interval=int(auto_refresh_s) * 1000, key="autorefresh")

# # ---------------------------
# # 4) Load data
# # ---------------------------
# df = load_parquet_folder(parquet_path)

# if df.empty:
#     st.warning(t["no_data"])
#     st.stop()

# # Normalize potential time columns early
# if "event_time" in df.columns:
#     df["event_time_dt"] = pd.to_datetime(df["event_time"], errors="coerce", utc=False)

# for c in ["event_ts", "spark_ts"]:
#     if c in df.columns:
#         df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)

# ts_base = pick_timestamp_column(df)

# if ts_base is None:
#     st.info(t["no_ts_found"])

# # ---------------------------
# # 5) Filters
# # ---------------------------

# # (A) Time window filter (PRO): based on UTC now()
# # if ts_base is not None:
# #     now = pd.Timestamp.utcnow()
# #     cutoff = now - pd.Timedelta(minutes=int(min_minutes))
# #     df = df[df[ts_base] >= cutoff]

# # 5) Filter by time window if possible
# if ts_base is not None:
#     # Force datetime and REMOVE timezone if present (make it naive)
#     df[ts_base] = pd.to_datetime(df[ts_base], errors="coerce")

#     # If timezone-aware -> convert to naive
#     if hasattr(df[ts_base].dt, "tz") and df[ts_base].dt.tz is not None:
#         df[ts_base] = df[ts_base].dt.tz_convert(None)

#     now_ts = df[ts_base].max()

#     if pd.notna(now_ts):
#         cutoff = now_ts - pd.Timedelta(minutes=int(min_minutes))

#         # cutoff must also be naive
#         cutoff = pd.to_datetime(cutoff).tz_localize(None) if getattr(cutoff, "tzinfo", None) else cutoff

#         df = df[df[ts_base] >= cutoff]
# else:
#     st.info(t["no_ts_found"])

# # If after time filtering df empty => clear message
# if df.empty:
#     st.warning(t["no_events_in_window"])
#     st.stop()

# # (B) Score filter
# if "score" in df.columns:
#     df = df[pd.to_numeric(df["score"], errors="coerce").fillna(0) >= float(min_score)]

# # (C) Source filter
# source_col = "source_id" if "source_id" in df.columns else None
# if source_col:
#     sources = sorted(df[source_col].dropna().unique().tolist())
#     selected = st.sidebar.selectbox(t["source_filter"], ["ALL"] + sources, index=0)
#     if selected != "ALL":
#         df = df[df[source_col] == selected]

# # If after all filters df empty
# if df.empty:
#     st.warning(t["no_events_in_window"])
#     st.stop()

# # ---------------------------
# # 6) KPIs
# # ---------------------------
# kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

# lat_p50 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 50)
# lat_p95 = percentile_safe(df.get("e2e_latency_ms", pd.Series(dtype=float)), 95)
# proc_p50 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 50)
# proc_p95 = percentile_safe(df.get("processing_time_ms", pd.Series(dtype=float)), 95)

# kpi1.metric(t["lat_p50"], f"{lat_p50:.1f}" if not np.isnan(lat_p50) else "—")
# kpi2.metric(t["lat_p95"], f"{lat_p95:.1f}" if not np.isnan(lat_p95) else "—")
# kpi3.metric(t["proc_p50"], f"{proc_p50:.1f}" if not np.isnan(proc_p50) else "—")
# kpi4.metric(t["proc_p95"], f"{proc_p95:.1f}" if not np.isnan(proc_p95) else "—")
# kpi5.metric(t["events_rate"], f"{len(df)}")

# st.caption(f"{t['last_updated']}: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

# st.divider()

# # ---------------------------
# # 7) Main charts + table
# # ---------------------------
# left, right = st.columns([1.2, 1])

# with left:
#     st.subheader(t["latest_events"])

#     # Display a clean "pro" subset
#     cols = ["event_time_dt", "event_ts", "spark_ts", "source_id", "frame_id", "emotion", "score", "e2e_latency_ms"]
#     visible_cols = [c for c in cols if c in df.columns]

#     df_view = df.sort_values(ts_base, ascending=False) if ts_base is not None else df
#     st.dataframe(df_view[visible_cols].head(50), use_container_width=True)

# with right:
#     st.subheader(t["distribution"])
#     if "emotion" in df.columns:
#         emo_counts = df["emotion"].astype(str).value_counts().reset_index()
#         emo_counts.columns = ["emotion", "count"]
#         st.bar_chart(emo_counts.set_index("emotion"))
#     else:
#         st.info(t["no_emotion"])

# st.divider()

# # ---------------------------
# # 8) Timeline (PRO, Plotly)
# # ---------------------------
# st.subheader(t["timeline"])

# timeline_ts = ts_base

# if timeline_ts is not None and "emotion" in df.columns:
#     tmp = df.dropna(subset=[timeline_ts, "emotion"]).copy()
#     tmp["minute"] = pd.to_datetime(tmp[timeline_ts], errors="coerce").dt.floor("min")
#     tmp = tmp.dropna(subset=["minute"])

#     if tmp.empty:
#         st.info(t["timeline_needs"])
#     else:
#         counts = tmp.groupby(["minute", "emotion"]).size().reset_index(name="count")

#         fig = px.line(
#             counts,
#             x="minute",
#             y="count",
#             color="emotion",
#             markers=False,
#             title=t["timeline"],
#         )
#         st.plotly_chart(fig, use_container_width=True)
# else:
#     st.info(t["timeline_needs"])


import os
import pandas as pd
import numpy as np
import streamlit as st

from streamlit_autorefresh import st_autorefresh
import plotly.express as px

from dotenv import load_dotenv
load_dotenv()

# ---------------------------
# 1) I18N (EN/RU)
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
        "no_events_in_window": "No events in the selected time window. Send messages to Kafka or increase the time window.",
        "last_updated": "Last updated",
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
        "no_events_in_window": "В выбранном окне времени нет событий. Отправь сообщения в Kafka или увеличь окно времени.",
        "last_updated": "Последнее обновление",
    },
}

# ---------------------------
# 2) Helpers
# ---------------------------
@st.cache_data(ttl=2)
def load_parquet_folder(path: str) -> pd.DataFrame:
    """
    Load a parquet folder (Spark output directory) as a pandas DataFrame.
    Works on Windows for a directory path like: data/parquet_events_v2
    """
    if not path or not os.path.exists(path):
        return pd.DataFrame()

    try:
        # pandas can read parquet directory (dataset) if engine supports it
        return pd.read_parquet(path)
    except Exception:
        try:
            import pyarrow.dataset as ds
            dataset = ds.dataset(path, format="parquet")
            return dataset.to_table().to_pandas()
        except Exception:
            return pd.DataFrame()


def percentile_safe(series: pd.Series, q: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return np.nan
    return float(np.percentile(s, q))


def to_naive_datetime(s: pd.Series) -> pd.Series:
    """
    Convert a Series to pandas datetime (timezone-naive).
    Works even if input has timezone-aware timestamps.
    """
    s = pd.to_datetime(s, errors="coerce")
    try:
        # If timezone-aware -> drop tz to make it naive
        if getattr(s.dt, "tz", None) is not None:
            s = s.dt.tz_convert(None)
    except Exception:
        pass
    return s


def pick_timestamp_column(df: pd.DataFrame):
    """
    Pick the best available timestamp column, robustly.
    Priority: event_ts > spark_ts > event_time_dt > event_time
    Returns chosen column name OR None.
    """
    candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time"]
    for c in candidates:
        if c in df.columns:
            df[c] = to_naive_datetime(df[c])
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

default_path = os.getenv("PARQUET_PATH", "data/parquet_events_v2")
parquet_path = st.sidebar.text_input(t["data_path"], value=default_path)

min_minutes = st.sidebar.number_input(
    t["time_filter"], min_value=1, max_value=1440, value=60, step=5
)
min_score = st.sidebar.slider(
    t["min_score"], min_value=0.0, max_value=1.0, value=0.0, step=0.05
)

refresh_now = st.sidebar.button(t["refresh"])
auto_refresh_s = st.sidebar.number_input(
    t["auto_refresh"], min_value=0, max_value=3600, value=10, step=1
)
st.sidebar.caption(t["note"])

# Force reload when user clicks Refresh
if refresh_now:
    st.cache_data.clear()

# Auto refresh (PRO)
if int(auto_refresh_s) > 0:
    st_autorefresh(interval=int(auto_refresh_s) * 1000, key="autorefresh")

# ---------------------------
# 4) Load data
# ---------------------------
df = load_parquet_folder(parquet_path)

if df.empty:
    st.warning(t["no_data"])
    st.stop()

# Normalize potential time columns early (timezone-safe)
if "event_time" in df.columns:
    df["event_time_dt"] = to_naive_datetime(df["event_time"])

for c in ["event_ts", "spark_ts"]:
    if c in df.columns:
        df[c] = to_naive_datetime(df[c])

ts_base = pick_timestamp_column(df)

if ts_base is None:
    st.info(t["no_ts_found"])

# ---------------------------
# 5) Filters
# ---------------------------

# (A) Time window filter
if ts_base is not None:
    df[ts_base] = to_naive_datetime(df[ts_base])

    now_ts = df[ts_base].max()
    if pd.notna(now_ts):
        cutoff = now_ts - pd.Timedelta(minutes=int(min_minutes))

        cutoff = pd.to_datetime(cutoff)
        if getattr(cutoff, "tzinfo", None) is not None:
            cutoff = cutoff.tz_localize(None)

        df = df[df[ts_base] >= cutoff]
else:
    st.info(t["no_ts_found"])

# If after time filtering df empty => clear message
if df.empty:
    st.warning(t["no_events_in_window"])
    st.stop()

# (B) Score filter
if "score" in df.columns:
    df = df[pd.to_numeric(df["score"], errors="coerce").fillna(0) >= float(min_score)]

# (C) Source filter
source_col = "source_id" if "source_id" in df.columns else None
if source_col:
    sources = sorted(df[source_col].dropna().unique().tolist())
    selected = st.sidebar.selectbox(t["source_filter"], ["ALL"] + sources, index=0)
    if selected != "ALL":
        df = df[df[source_col] == selected]

# If after all filters df empty
if df.empty:
    st.warning(t["no_events_in_window"])
    st.stop()

# ---------------------------
# 6) KPIs
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

st.caption(f"{t['last_updated']}: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

st.divider()

# ---------------------------
# 7) Main charts + table
# ---------------------------
left, right = st.columns([1.2, 1])

with left:
    st.subheader(t["latest_events"])

    cols = ["event_time_dt", "event_ts", "spark_ts", "source_id", "frame_id", "emotion", "score", "e2e_latency_ms"]
    visible_cols = [c for c in cols if c in df.columns]

    if ts_base is not None and ts_base in df.columns and not df[ts_base].isna().all():
        df_view = df.sort_values(ts_base, ascending=False)
    else:
        df_view = df

    st.dataframe(df_view[visible_cols].head(50), use_container_width=True)

with right:
    st.subheader(t["distribution"])
    if "emotion" in df.columns:
        emo_counts = df["emotion"].astype(str).value_counts().reset_index()
        emo_counts.columns = ["emotion", "count"]
        st.bar_chart(emo_counts.set_index("emotion"))
    else:
        st.info(t["no_emotion"])

st.divider()

# ---------------------------
# 8) Timeline (PRO, Plotly)
# ---------------------------
st.subheader(t["timeline"])

timeline_ts = ts_base

if timeline_ts is not None and "emotion" in df.columns:
    tmp = df.dropna(subset=[timeline_ts, "emotion"]).copy()
    tmp[timeline_ts] = to_naive_datetime(tmp[timeline_ts])

    tmp["minute"] = tmp[timeline_ts].dt.floor("5s")
    tmp = tmp.dropna(subset=["minute"])

    if tmp.empty:
        st.info(t["timeline_needs"])
    else:
        counts = tmp.groupby(["minute", "emotion"]).size().reset_index(name="count")

        fig = px.line(
            counts,
            x="minute",
            y="count",
            color="emotion",
            markers=False,
            title=t["timeline"],
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info(t["timeline_needs"])