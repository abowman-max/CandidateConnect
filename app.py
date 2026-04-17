from pathlib import Path
import pandas as pd
import streamlit as st
import gdown

st.set_page_config(page_title="Candidate Connect", layout="wide")

DRIVE_FILE_ID = "102YgV6ev74B_FutPsO1SIwUcyjZtETWa"
LOCAL_PARQUET = Path("/tmp/candidate_connect_data.parquet")

@st.cache_resource(show_spinner=True)
def load_data():
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"

    # Only download once if the file is already present
    if not LOCAL_PARQUET.exists() or LOCAL_PARQUET.stat().st_size == 0:
        gdown.download(url=url, output=str(LOCAL_PARQUET), quiet=False)

    if not LOCAL_PARQUET.exists() or LOCAL_PARQUET.stat().st_size == 0:
        st.error("Google Drive download failed. Check sharing settings.")
        st.stop()

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
