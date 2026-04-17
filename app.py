import streamlit as st
import pandas as pd
import requests
from io import BytesIO

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(page_title="Candidate Connect", layout="wide")

DRIVE_FILE_ID = "102YgV6ev74B_FutPsO1SIwUcyjZtETWa"

# ----------------------------
# LOAD DATA FROM GOOGLE DRIVE
# ----------------------------
@st.cache_data(show_spinner=True)
def load_data():
    url = f"https://drive.google.com/uc?export=download&id={DRIVE_FILE_ID}"

    response = requests.get(url, timeout=180, allow_redirects=True)
    response.raise_for_status()

    content_type = response.headers.get("content-type", "").lower()
    first_bytes = response.content[:50]

    # 🔴 Detect if Google returned a webpage instead of file
    if (
        "text/html" in content_type
        or first_bytes.startswith(b"<!DOCTYPE html")
        or first_bytes.startswith(b"<html")
    ):
        st.error("❌ Google Drive did not return the parquet file.\n\nMake sure the file is shared as:\n👉 'Anyone with the link can view'")
        st.stop()

    return pd.read_parquet(BytesIO(response.content))


# ----------------------------
# APP UI
# ----------------------------
st.title("Candidate Connect")

st.info("Loading data from Google Drive...")

try:
    df = load_data()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# ----------------------------
# SIMPLE TEST DISPLAY
# ----------------------------
st.success("✅ Data loaded successfully")

st.write("### Preview")
st.dataframe(df.head())

st.write("### Row Count")
st.write(len(df))
