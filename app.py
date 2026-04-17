import streamlit as st
import pandas as pd
import gdown
from pathlib import Path

st.set_page_config(page_title="Candidate Connect", layout="wide")

# ✅ Your new SMALL dataset file ID
DRIVE_FILE_ID = "1vQTn2pc1vuZiI8a0CyPvPA1k3jMOSNPt"

LOCAL_PARQUET = Path("/tmp/candidate_connect_data.parquet")

@st.cache_resource(show_spinner=True)
def load_data():
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"

    if not LOCAL_PARQUET.exists():
        gdown.download(url=url, output=str(LOCAL_PARQUET), quiet=False)

    return pd.read_parquet(LOCAL_PARQUET)

st.title("Candidate Connect")

st.info("Loading data from Google Drive...")

try:
    df = load_data()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

st.success("Data loaded successfully")

st.write("### Preview")
st.dataframe(df.head())

st.write("### Row Count")
st.write(len(df))
