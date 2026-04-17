import streamlit as st
import pandas as pd
import gdown
from pathlib import Path

st.set_page_config(page_title="Candidate Connect", layout="wide")

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

# ----------------------------
# SIMPLE DASHBOARD
# ----------------------------

col1, col2 = st.columns(2)

with col1:
    st.metric("Total Voters", len(df))

with col2:
    st.metric("Unique Counties", df["County"].nunique())

st.write("### Party Breakdown")
party_counts = df["Party"].value_counts()
st.bar_chart(party_counts)

st.write("### Gender Breakdown")
gender_counts = df["Gender"].value_counts()
st.bar_chart(gender_counts)

st.write("### Age Range Breakdown")
age_counts = df["Age_Range"].value_counts()
st.bar_chart(age_counts)

st.write("### Preview")
st.dataframe(df.head())
