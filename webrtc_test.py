import streamlit as st
from streamlit_webrtc import webrtc_streamer, WebRtcMode, RTCConfiguration

st.set_page_config(layout="wide")
st.title("WebRTC test")

webrtc_streamer(
    key="test",
    mode=WebRtcMode.SENDRECV,
    rtc_configuration=RTCConfiguration({"iceServers": []}),
    media_stream_constraints={"video": True, "audio": False},
    async_processing=True,
)