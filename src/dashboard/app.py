import os
import pandas as pd
import streamlit as st

st.set_page_config(page_title="VKR Dashboard", layout="wide")
st.title("📊 VKR — Streaming Analytics Dashboard")

base_path = "data"  # Windows folder in your repo
options = {
    "parquet_windows (messages test)": os.path.join(base_path, "parquet_windows"),
    "parquet_events (JSON events)": os.path.join(base_path, "parquet_events"),
}

choice = st.sidebar.selectbox("Dataset", list(options.keys()))
path = options[choice]

st.sidebar.write("Path:", path)
refresh = st.sidebar.button("🔄 Refresh")

@st.cache_data(show_spinner=False)
def load_parquet(folder):
    return pd.read_parquet(folder)

if refresh:
    st.cache_data.clear()

if not os.path.exists(path):
    st.error(f"Folder not found: {path}")
    st.stop()

df = load_parquet(path)

st.subheader("Preview")
st.dataframe(df.tail(50), use_container_width=True)

st.subheader("Counts")
if "emotion" in df.columns:
    col1, col2 = st.columns(2)
    with col1:
        st.write("Emotion counts")
        st.bar_chart(df["emotion"].value_counts())
    with col2:
        st.write("Top sources")
        if "source_id" in df.columns:
            st.bar_chart(df["source_id"].value_counts())
else:
    st.write("Message counts")
    st.bar_chart(df["message"].value_counts())

st.caption("Tip: keep Spark running and send events from Jupyter, then click Refresh.")
