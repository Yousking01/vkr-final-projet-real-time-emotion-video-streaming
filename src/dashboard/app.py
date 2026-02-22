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


# import os
# import pandas as pd
# import numpy as np
# import streamlit as st

# from streamlit_autorefresh import st_autorefresh
# import plotly.express as px

# from dotenv import load_dotenv
# load_dotenv()

# # ---------------------------
# # 1) I18N (EN/RU)
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

#     try:
#         # pandas can read parquet directory (dataset) if engine supports it
#         return pd.read_parquet(path)
#     except Exception:
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


# def to_naive_datetime(s: pd.Series) -> pd.Series:
#     """
#     Convert a Series to pandas datetime (timezone-naive).
#     Works even if input has timezone-aware timestamps.
#     """
#     s = pd.to_datetime(s, errors="coerce")
#     try:
#         # If timezone-aware -> drop tz to make it naive
#         if getattr(s.dt, "tz", None) is not None:
#             s = s.dt.tz_convert(None)
#     except Exception:
#         pass
#     return s


# def pick_timestamp_column(df: pd.DataFrame):
#     """
#     Pick the best available timestamp column, robustly.
#     Priority: event_ts > spark_ts > event_time_dt > event_time
#     Returns chosen column name OR None.
#     """
#     candidates = ["event_ts", "spark_ts", "event_time_dt", "event_time"]
#     for c in candidates:
#         if c in df.columns:
#             df[c] = to_naive_datetime(df[c])
#             if not df[c].isna().all():
#                 return c
#     return None


# # ---------------------------
# # 3) UI
# # ---------------------------
# st.set_page_config(page_title="Emotion Dashboard", layout="wide")

# lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
# t = I18N[lang]

# st.title(t["app_title"])
# st.sidebar.header(t["sidebar_title"])

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

# # Normalize potential time columns early (timezone-safe)
# if "event_time" in df.columns:
#     df["event_time_dt"] = to_naive_datetime(df["event_time"])

# for c in ["event_ts", "spark_ts"]:
#     if c in df.columns:
#         df[c] = to_naive_datetime(df[c])

# ts_base = pick_timestamp_column(df)

# if ts_base is None:
#     st.info(t["no_ts_found"])

# # ---------------------------
# # 5) Filters
# # ---------------------------

# # (A) Time window filter
# if ts_base is not None:
#     df[ts_base] = to_naive_datetime(df[ts_base])

#     now_ts = df[ts_base].max()
#     if pd.notna(now_ts):
#         cutoff = now_ts - pd.Timedelta(minutes=int(min_minutes))

#         cutoff = pd.to_datetime(cutoff)
#         if getattr(cutoff, "tzinfo", None) is not None:
#             cutoff = cutoff.tz_localize(None)

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

#     cols = ["event_time_dt", "event_ts", "spark_ts", "source_id", "frame_id", "emotion", "score", "e2e_latency_ms"]
#     visible_cols = [c for c in cols if c in df.columns]

#     if ts_base is not None and ts_base in df.columns and not df[ts_base].isna().all():
#         df_view = df.sort_values(ts_base, ascending=False)
#     else:
#         df_view = df

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
#     tmp[timeline_ts] = to_naive_datetime(tmp[timeline_ts])

#     tmp["minute"] = tmp[timeline_ts].dt.floor("5s")
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


# app.py
# Streamlit 1.54+ — Production-ready SaaS layout
# - Stable tabs via st.tabs (NO radio hacks, no double underline)
# - Premium dark sidebar
# - Top header (entête) above tabs
# - Tabs directly under header
# - Logo + back icon just below header (inside the topbar)
# - Gray "Last updated" banner
# - Blue band above "Latest Events" + styled dataframe header
# - Plotly emotion colors fixed
# - Kafka/Parquet contract unchanged

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

# Live webcam
import cv2
import av
from streamlit_webrtc import webrtc_streamer, WebRtcMode, RTCConfiguration

import mediapipe as mp
from mediapipe.tasks import python as mp_python
from mediapipe.tasks.python import vision

# Kafka
from kafka import KafkaProducer

load_dotenv()

# ---------------------------
# I18N (EN/RU)
# ---------------------------
I18N = {
    "en": {
        "sidebar_title": "Controls",
        "language": "Language",
        "tab_analytics": "Analytics (Parquet)",
        "tab_live": "Live Webcam (Windows)",
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

        "live_help": "Webcam + face boxes + emotion label + optional Kafka send",
        "live_source_id": "source_id",
        "live_send_kafka": "Send events to Kafka",
        "live_send_interval": "Min seconds between sends",
        "live_kafka_bootstrap": "Kafka bootstrap",
        "live_topic": "Kafka topic",
        "live_status": "Status",
        "payload_fields": "Payload fields (JSON)",
        "expected": "✅ Expected: webcam with rectangles. If Kafka ON, Spark writes Parquet and it appears in Analytics.",

        "mp_missing": "MediaPipe Tasks not available. Run: pip install -U mediapipe",
        "mp_model_missing": "Face model .tflite missing. Put it in ./models/ or set MP_FACE_MODEL_PATH in .env",
    },
    "ru": {
        "sidebar_title": "Управление",
        "language": "Язык",
        "tab_analytics": "Аналитика (Parquet)",
        "tab_live": "Веб-камера (Windows)",
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

        "live_help": "Веб-камера + рамки лиц + эмоции + отправка в Kafka (опционально)",
        "live_source_id": "source_id",
        "live_send_kafka": "Отправлять события в Kafka",
        "live_send_interval": "Мин. секунды между отправками",
        "live_kafka_bootstrap": "Kafka bootstrap",
        "live_topic": "Kafka topic",
        "live_status": "Статус",
        "payload_fields": "Поля payload (JSON)",
        "expected": "✅ Ожидаемо: веб-камера с рамками. Если Kafka ON, Spark пишет Parquet и это видно в Analytics.",

        "mp_missing": "MediaPipe Tasks недоступен. Установи: pip install -U mediapipe",
        "mp_model_missing": "Нет модели лица .tflite. Положи в ./models/ или задай MP_FACE_MODEL_PATH в .env",
    },
}

# ---------------------------
# Premium CSS (tabs + sidebar + header + animations)
# ---------------------------
APP_CSS = r"""
<style>
/* Page width */
.block-container {
  max-width: none !important;
  padding-top: 0.70rem;
  padding-left: 2.2rem;
  padding-right: 2.2rem;
}

/* Hide Streamlit menu/footer */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

/* ===== Premium dark sidebar ===== */
section[data-testid="stSidebar"]{
  background: linear-gradient(180deg, #1b1e26 0%, #202533 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.06);
}
section[data-testid="stSidebar"] *{
  color: rgba(255,255,255,0.92) !important;
}
section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3 {
  color: rgba(255,255,255,0.95) !important;
  letter-spacing: 0.2px;
}
section[data-testid="stSidebar"] label p{
  color: rgba(255,255,255,0.72) !important;
  font-weight: 750 !important;
}

/* Inputs style */
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
section[data-testid="stSidebar"] div[data-baseweb="slider"]{
  padding-top: 6px !important;
}

/* Sidebar button */
section[data-testid="stSidebar"] button[kind="secondary"],
section[data-testid="stSidebar"] button[kind="primary"],
section[data-testid="stSidebar"] button {
  border-radius: 14px !important;
  border: 1px solid rgba(255,255,255,0.14) !important;
  background: rgba(255,255,255,0.06) !important;
  color: rgba(255,255,255,0.92) !important;
  font-weight: 900 !important;
  transition: transform .14s ease, box-shadow .14s ease, background .14s ease;
}
section[data-testid="stSidebar"] button:hover{
  transform: translateY(-1px);
  background: rgba(255,255,255,0.10) !important;
  box-shadow: 0 10px 24px rgba(0,0,0,0.25);
}

/* ===== Entête (header) FIX (no crop) ===== */
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

.app-header .brand-title{
  font-size: 28px;
  font-weight: 950;
  color: rgba(255,255,255,0.96);
  line-height: 1.15;
  margin: 0;
  padding: 0;
}

.app-header .brand-sub{
  font-size: 14px;
  font-weight: 750;
  color: rgba(255,255,255,0.72);
  margin-top: 6px;
}

div[data-testid="stAppViewContainer"]{
  overflow: visible !important;
}
.block-container{
  padding-top: 1.2rem !important;
}

/* ===== Topbar (logo + back under header) ===== */
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
.backbtn {
  width: 36px; height: 36px;
  border-radius: 10px;
  display:flex; align-items:center; justify-content:center;
  background: rgba(255,255,255,0.10);
  border: 1px solid rgba(255,255,255,0.18);
  font-size: 18px;
  font-weight: 900;
  transition: transform .16s ease, background .16s ease;
  user-select: none;
}
.backbtn:hover{ transform: translateY(-1px); background: rgba(255,255,255,0.14); }

/* Animated logo */
@keyframes spinGlow {
  0% { transform: rotate(0deg); filter: drop-shadow(0 0 0 rgba(123,220,255,0.0)); }
  50% { transform: rotate(180deg); filter: drop-shadow(0 8px 18px rgba(123,220,255,0.22)); }
  100% { transform: rotate(360deg); filter: drop-shadow(0 0 0 rgba(123,220,255,0.0)); }
}
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
  animation: spinGlow 6s linear infinite;
}
.topbar .title { font-size: 24px; font-weight: 950; line-height: 1.05; }
.topbar .subtitle { font-size: 13px; opacity: 0.88; margin-top: 4px; }
.topbar .right { font-size: 12px; opacity: 0.85; }

/* ===== Streamlit Tabs -> SaaS style (stable) ===== */
div[data-testid="stTabs"]{
  margin-top: 2px;
}
div[data-testid="stTabs"] [role="tablist"]{
  gap: 34px;
  border-bottom: 1px solid rgba(255,255,255,0.10);
  padding-bottom: 10px;
}
div[data-testid="stTabs"] button[role="tab"]{
  background: transparent !important;
  border: none !important;
  padding: 8px 0 !important;
  margin: 0 !important;

  font-size: 22px !important;
  font-weight: 950 !important;
  color: rgba(255,255,255,0.34) !important;

  border-bottom: 4px solid transparent !important;
  transition: transform .12s ease, color .12s ease, border-color .12s ease;
}
div[data-testid="stTabs"] button[role="tab"]:hover{
  transform: translateY(-1px);
  color: rgba(255,255,255,0.55) !important;
}
div[data-testid="stTabs"] button[role="tab"][aria-selected="true"]{
  color: #e11d48 !important;
  border-bottom: 4px solid #e11d48 !important;
}
div[data-testid="stTabs"] div[data-baseweb="tab-highlight"]{
  display:none !important;
}

/* ===== Cards + hover ===== */
.kpi-card, .section-card, .gray-banner, .blue-band{
  transition: transform .14s ease, box-shadow .14s ease, border-color .14s ease;
}
.kpi-card:hover, .section-card:hover{
  transform: translateY(-2px);
  box-shadow: 0 16px 32px rgba(0,0,0,0.10);
  border-color: rgba(59,130,246,0.24);
}
.gray-banner:hover, .blue-band:hover{
  transform: translateY(-1px);
  box-shadow: 0 12px 26px rgba(0,0,0,0.08);
}

/* KPI cards */
.kpi-card{
  background: #ffffff;
  border: 1px solid rgba(15,23,42,0.08);
  border-radius: 14px;
  padding: 12px 14px;
  box-shadow: 0 8px 20px rgba(15,23,42,0.06);
  height: 78px;
}
.kpi-label{ font-size: 12px; color: rgba(15,23,42,0.70); font-weight: 900; margin-bottom: 6px; }
.kpi-value{ font-size: 30px; font-weight: 950; color: #0f172a; line-height: 1.0; }
.kpi-unit{ font-size: 14px; font-weight: 900; color: rgba(15,23,42,0.65); margin-left: 6px; }

/* Section card */
.section-card{
  background:#ffffff;
  border:1px solid rgba(15,23,42,0.08);
  border-radius: 14px;
  padding: 12px 14px;
  box-shadow: 0 8px 20px rgba(15,23,42,0.06);
}
hr.soft{ border:none;height:1px; background: rgba(15,23,42,0.10); margin: 14px 0; }

/* Gray banner */
.gray-banner{
  width: 100%;
  background: #f1f3f6;
  border: 1px solid rgba(15,23,42,0.08);
  border-radius: 12px;
  padding: 10px 12px;
  color: rgba(15,23,42,0.78);
  font-weight: 850;
  margin-top: 10px;
}

/* Blue band header */
.blue-band{
  width: 100%;
  background: linear-gradient(90deg, #2b5aa8, #2f6bc7);
  color: white;
  border-radius: 12px;
  padding: 10px 12px;
  font-weight: 950;
  margin-bottom: 10px;
  box-shadow: 0 8px 18px rgba(43,90,168,0.18);
}

/* Live left panel */
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
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
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
# Kafka helpers (contract unchanged)
# ---------------------------
@st.cache_resource
def get_kafka_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=2,
        acks=1,
        linger_ms=10,
    )

def safe_send_kafka(producer: KafkaProducer, topic: str, event: dict) -> Tuple[bool, str]:
    try:
        producer.send(topic, event)
        producer.flush(timeout=1)
        return True, "sent"
    except Exception as e:
        return False, f"Kafka send failed: {e}"

# ---------------------------
# Emotion stub + palette
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
# Face detection (MediaPipe Tasks) ✅ FIXED (no mp.solutions)
# ---------------------------
# @st.cache_resource
# def get_mp_face_detector():
#     """
#     Uses MediaPipe Tasks API (FaceDetector).
#     Requires a .tflite model file.
#     """
#     try:
#         from mediapipe.tasks import python as mp_python
#         from mediapipe.tasks.python import vision as mp_vision
#     except Exception as e:
#         raise RuntimeError(f"MediaPipe Tasks not available: {e}")

#     model_path = os.getenv("MP_FACE_MODEL_PATH", "models/blaze_face_short_range.tflite")
#     if not os.path.exists(model_path):
#         raise FileNotFoundError(model_path)

#     base_options = mp_python.BaseOptions(model_asset_path=model_path)
#     options = mp_vision.FaceDetectorOptions(base_options=base_options)
#     return mp_vision.FaceDetector.create_from_options(options)

# _detector_lock = threading.Lock()

# def detect_faces_mp(detector, frame_rgb: np.ndarray) -> List[Tuple[int, int, int, int, float]]:
#     """
#     Returns list of (x, y, w, h, score) in PIXELS.
#     frame_rgb must be RGB uint8.
#     """
#     from mediapipe.tasks.python import vision as mp_vision

#     h, w, _ = frame_rgb.shape
#     mp_image = mp_vision.Image(image_format=mp_vision.ImageFormat.SRGB, data=frame_rgb)

#     with _detector_lock:
#         result = detector.detect(mp_image)

#     boxes: List[Tuple[int, int, int, int, float]] = []
#     if result and result.detections:
#         for det in result.detections:
#             bbox = det.bounding_box
#             score = float(det.categories[0].score) if det.categories else 0.0

#             x = max(0, int(bbox.origin_x))
#             y = max(0, int(bbox.origin_y))
#             ww = max(1, int(bbox.width))
#             hh = max(1, int(bbox.height))

#             ww = min(ww, w - x)
#             hh = min(hh, h - y)
#             boxes.append((x, y, ww, hh, score))
#     return boxes

# ---------------------------
# Face detection (MediaPipe) - robust (solutions OR tasks+tflite)
# ---------------------------
# @st.cache_resource
# def get_mp_face_detector():
#     """
#     Returns a dict:
#       {"backend": "solutions", "detector": <FaceDetection>}
#     or
#       {"backend": "tasks", "detector": <FaceDetector>}
#     """
#     model_path = os.getenv("MP_FACE_MODEL_PATH", "models/blaze_face_short_range.tflite")

#     # 1) Try classic mp.solutions (no external tflite needed)
#     try:
#         if hasattr(mp, "solutions"):
#             mp_fd = mp.solutions.face_detection
#             det = mp_fd.FaceDetection(model_selection=0, min_detection_confidence=0.5)
#             return {"backend": "solutions", "detector": det}
#     except Exception:
#         pass

#     # 2) Fallback: MediaPipe Tasks (uses .tflite model)
#     # Requires: mediapipe installed in the SAME venv as streamlit
#     from mediapipe.tasks.python.core.base_options import BaseOptions
#     from mediapipe.tasks.python import vision
#     from mediapipe.tasks.python.vision import RunningMode

#     if not os.path.exists(model_path):
#         raise FileNotFoundError(
#             f"MP_FACE_MODEL_PATH not found: {model_path}. "
#             f"Download the model into models/ and set MP_FACE_MODEL_PATH in .env."
#         )

#     options = vision.FaceDetectorOptions(
#         base_options=BaseOptions(model_asset_path=model_path),
#         running_mode=RunningMode.IMAGE,
#         min_detection_confidence=0.5,
#     )
#     det = vision.FaceDetector.create_from_options(options)
#     return {"backend": "tasks", "detector": det}


# def detect_faces_mp(det_bundle, frame_rgb: np.ndarray) -> List[Tuple[int, int, int, int, float]]:
#     """
#     Returns list of (x, y, w, h, confidence).
#     Works for both backends.
#     """
#     h, w, _ = frame_rgb.shape
#     backend = det_bundle["backend"]
#     det = det_bundle["detector"]
#     boxes = []

#     if backend == "solutions":
#         res = det.process(frame_rgb)
#         if res.detections:
#             for d in res.detections:
#                 conf = float(d.score[0]) if d.score else 0.0
#                 bb = d.location_data.relative_bounding_box
#                 x = max(0, int(bb.xmin * w))
#                 y = max(0, int(bb.ymin * h))
#                 ww = max(1, int(bb.width * w))
#                 hh = max(1, int(bb.height * h))
#                 ww = min(ww, w - x)
#                 hh = min(hh, h - y)
#                 boxes.append((x, y, ww, hh, conf))
#         return boxes

#     # backend == "tasks"
#     mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame_rgb)
#     result = det.detect(mp_image)
#     if result.detections:
#         for d in result.detections:
#             conf = float(d.categories[0].score) if d.categories else 0.0
#             bb = d.bounding_box  # origin_x, origin_y, width, height
#             x = max(0, int(bb.origin_x))
#             y = max(0, int(bb.origin_y))
#             ww = max(1, int(bb.width))
#             hh = max(1, int(bb.height))
#             ww = min(ww, w - x)
#             hh = min(hh, h - y)
#             boxes.append((x, y, ww, hh, conf))
#     return boxes

# @st.cache_resource
# def get_mp_face_detector():
#     model_path = os.getenv("MP_FACE_MODEL_PATH", "models/blaze_face_short_range.tflite")
#     if not os.path.exists(model_path):
#         raise FileNotFoundError(
#             f"Model not found: {model_path}\n"
#             f"Download blaze_face_short_range.tflite into models/ and/or set MP_FACE_MODEL_PATH in .env"
#         )

#     base_options = mp_python.BaseOptions(model_asset_path=model_path)
#     options = vision.FaceDetectorOptions(
#         base_options=base_options,
#         min_detection_confidence=0.5,
#     )
#     return vision.FaceDetector.create_from_options(options)


# def detect_faces_mp(detector, frame_rgb: np.ndarray) -> List[Tuple[int, int, int, int, float]]:
#     """
#     Returns list of (x, y, w, h, conf) in PIXELS.
#     """
#     h, w, _ = frame_rgb.shape

#     # MediaPipe expects SRGB
#     mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame_rgb)
#     result = detector.detect(mp_image)

#     boxes: List[Tuple[int, int, int, int, float]] = []
#     if not result or not getattr(result, "detections", None):
#         return boxes

#     for det in result.detections:
#         bb = det.bounding_box  # origin_x, origin_y, width, height (px)

#         x = int(max(0, bb.origin_x))
#         y = int(max(0, bb.origin_y))
#         ww = int(max(1, bb.width))
#         hh = int(max(1, bb.height))

#         # clamp inside image
#         ww = min(ww, w - x)
#         hh = min(hh, h - y)

#         # score (robust)
#         conf = 0.0
#         if getattr(det, "categories", None) and len(det.categories) > 0:
#             conf = float(det.categories[0].score)
#         elif getattr(det, "score", None):
#             # fallback if structure differs
#             try:
#                 conf = float(det.score[0])
#             except Exception:
#                 conf = 0.0

#         boxes.append((x, y, ww, hh, conf))

#     return boxes

# ---------------------------
# Face detection (MediaPipe OR OpenCV fallback)
# ---------------------------
@st.cache_resource
def get_face_detector():
    # Try MediaPipe "solutions" if available
    try:
        if hasattr(mp, "solutions"):
            mp_fd = mp.solutions.face_detection
            det = mp_fd.FaceDetection(model_selection=0, min_detection_confidence=0.5)
            return ("mediapipe", det)
    except Exception:
        pass

    # Fallback: OpenCV Haar cascade (works everywhere on Windows)
    cascade_path = os.path.join(cv2.data.haarcascades, "haarcascade_frontalface_default.xml")
    cascade = cv2.CascadeClassifier(cascade_path)
    return ("opencv", cascade)

def detect_faces(det_tuple, frame_rgb: np.ndarray):
    kind, det = det_tuple
    h, w, _ = frame_rgb.shape
    boxes = []

    if kind == "mediapipe":
        res = det.process(frame_rgb)
        if res.detections:
            for d in res.detections:
                conf = float(d.score[0]) if d.score else 0.0
                bb = d.location_data.relative_bounding_box
                x = max(0, int(bb.xmin * w))
                y = max(0, int(bb.ymin * h))
                ww = max(1, int(bb.width * w))
                hh = max(1, int(bb.height * h))
                ww = min(ww, w - x)
                hh = min(hh, h - y)
                boxes.append((x, y, ww, hh, conf))
        return boxes

    # OpenCV Haar fallback
    gray = cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2GRAY)
    faces = det.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(60, 60))
    for (x, y, ww, hh) in faces:
        boxes.append((int(x), int(y), int(ww), int(hh), 0.80))
    return boxes

# ---------------------------
# UI
# ---------------------------
st.set_page_config(
    page_title="Emotion Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(APP_CSS, unsafe_allow_html=True)

# Sidebar language + header
lang = st.sidebar.selectbox("Language / Язык", ["en", "ru"], index=0)
t = I18N[lang]
st.sidebar.header(t["sidebar_title"])

# ===== Entête (header) on top =====
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
        <div class="badge">Production · v2</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ===== Tabs directly under header (stable) =====
tab_analytics, tab_live = st.tabs([t["tab_analytics"], t["tab_live"]])

def render_topbar(title: str):
    header_subtitle = "Spark → Parquet (Analytics)  |  Webcam → Kafka (Live)"
    st.markdown(
        f"""
        <div class="topbar">
          <div class="left">
            <div class="backbtn">&lt;</div>
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
        st.stop()

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
        st.stop()

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
        st.stop()

    # KPIs
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

        st.dataframe(style_latest_events(view), use_container_width=True, height=390)
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

# ============================================================
# TAB 2: Live Webcam
# ============================================================
with tab_live:
    render_topbar(t["tab_live"])

    left_panel, video_panel = st.columns([0.42, 1.0], gap="large")

    with left_panel:
        st.markdown("<div class='left-panel'>", unsafe_allow_html=True)
        st.caption(t["live_help"])

        default_bootstrap = os.getenv("KAFKA_BOOTSTRAP_WIN", "localhost:9092")
        default_topic = os.getenv("KAFKA_TOPIC", "test_topic")

        live_source_id = st.text_input(t["live_source_id"], value="cam_01")
        live_send_kafka = st.checkbox(t["live_send_kafka"], value=True)
        min_send_interval = st.number_input(t["live_send_interval"], min_value=0.0, max_value=5.0, value=0.5, step=0.1)

        kafka_bootstrap = st.text_input(t["live_kafka_bootstrap"], value=default_bootstrap)
        kafka_topic = st.text_input(t["live_topic"], value=default_topic)

        producer = None
        status_ok = False
        status_msg = ""

        if live_send_kafka:
            try:
                producer = get_kafka_producer(kafka_bootstrap)
                status_ok = True
                status_msg = f"Kafka producer OK ({kafka_bootstrap})"
            except Exception as e:
                producer = None
                status_ok = False
                status_msg = f"Kafka producer error: {e}"
        else:
            status_ok = True
            status_msg = "Kafka send disabled"

        if status_ok:
            st.markdown(f"<div class='status-ok'>✅ {t['live_status']}: {status_msg}</div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='status-bad'>❌ {t['live_status']}: {status_msg}</div>", unsafe_allow_html=True)

        st.markdown(f"<div class='checklist'><div class='section-title'>{t['payload_fields']}</div>", unsafe_allow_html=True)
        fields = [
            "source_id", "frame_id", "face_id", "producer_ts",
            "emotion, score", "bbox_x, bbox_y, bbox_w, bbox_h",
        ]
        for f in fields:
            st.markdown(f"<div class='item'><span class='tick'>✓</span><span>{f}</span></div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ---- detector init (safe) ----
    try:
        # detector = get_mp_face_detector()
        detector = get_face_detector()
    except FileNotFoundError:
        st.error(t["mp_model_missing"])
        st.stop()
    except Exception as e:
        st.error(f"{t['mp_missing']}\n\nDetails: {e}")
        st.stop()

    lock = threading.Lock()
    state = {"last_sent": 0.0, "frame_id": 100000}

    def video_frame_callback(frame: av.VideoFrame) -> av.VideoFrame:
        img_bgr = frame.to_ndarray(format="bgr24")
        img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)

        # boxes = detect_faces_mp(detector, img_rgb)
        # boxes = detect_faces(detector, img_rgb)
        boxes = detect_faces(detector, img_rgb)

        for (x, y, w, h, conf) in boxes:
            face_rgb = img_rgb[y:y+h, x:x+w]
            emo, score = infer_emotion_stub(face_rgb)

            cv2.rectangle(img_bgr, (x, y), (x + w, y + h), (0, 255, 0), 2)
            label = f"{emo.capitalize()} ({score:.2f})"
            cv2.putText(img_bgr, label, (x, max(26, y - 10)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0, 255, 0), 2)

            if live_send_kafka:
                # producer is cached; might be None if init failed
                try:
                    producer_local = get_kafka_producer(kafka_bootstrap)
                except Exception:
                    producer_local = None

                if producer_local is not None:
                    now = time.time()
                    with lock:
                        can_send = (now - state["last_sent"]) >= float(min_send_interval)
                        if can_send:
                            state["last_sent"] = now
                            state["frame_id"] += 1
                            fid = state["frame_id"]
                        else:
                            fid = None

                    if fid is not None:
                        event = {
                            "source_id": live_source_id,
                            "frame_id": int(fid),
                            "face_id": 1,
                            "producer_ts": float(now),
                            "emotion": emo,
                            "score": float(score),
                            "bbox_x": int(x),
                            "bbox_y": int(y),
                            "bbox_w": int(w),
                            "bbox_h": int(h),
                        }
                        safe_send_kafka(producer_local, kafka_topic, event)

        return av.VideoFrame.from_ndarray(img_bgr, format="bgr24")

    with video_panel:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        # RTC_CONFIGURATION = RTCConfiguration({"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]})
        RTC_CONFIGURATION = RTCConfiguration({"iceServers": []})
        webrtc_streamer(
        key="live-webcam",
        mode=WebRtcMode.SENDRECV,
        rtc_configuration=RTC_CONFIGURATION,
        media_stream_constraints={"video": True, "audio": False},
        video_frame_callback=video_frame_callback,
        async_processing=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
        st.info(t["expected"])