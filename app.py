import streamlit as st
import pandas as pd
import gdown
from pathlib import Path

st.set_page_config(page_title="Candidate Connect", layout="wide")

DRIVE_FILE_ID = "1vQTn2pc1vuZiI8a0CyPvPA1k3jMOSNPt"
LOCAL_PARQUET = Path("/tmp/candidate_connect_data.parquet")

def smart_title(val):
    if pd.isna(val):
        return ""
    text = str(val).strip()
    if not text or text.lower() == "nan":
        return ""
    return " ".join(word.capitalize() for word in text.replace("_", " ").split())

@st.cache_resource(show_spinner=True)
def load_data():
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"
    if not LOCAL_PARQUET.exists():
        gdown.download(url=url, output=str(LOCAL_PARQUET), quiet=False)
    df = pd.read_parquet(LOCAL_PARQUET)
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]

    status_col = "VoterStatus" if "VoterStatus" in df.columns else ("voterstatus" if "voterstatus" in df.columns else None)
    if status_col:
        df = df[df[status_col].astype(str).str.strip().str.upper() == "A"].copy()

    for col in ["County", "Municipality", "Precinct", "School District", "CalculatedParty"]:
        if col in df.columns:
            df[col] = df[col].astype("object").map(smart_title)

    if "Age" in df.columns:
        df["_AgeNum"] = pd.to_numeric(df["Age"], errors="coerce")
    else:
        df["_AgeNum"] = pd.NA

    if "Party" in df.columns:
        df["Party"] = df["Party"].astype(str).str.strip().replace({"nan": "O", "None": "O", "": "O", "U": "O"})

    gender_col = "Gender" if "Gender" in df.columns else ("Sex" if "Sex" in df.columns else None)
    if gender_col:
        df["_Gender"] = df[gender_col].astype(str).str.strip().replace({"nan": "U", "None": "U", "": "U"}).str.upper()
    else:
        df["_Gender"] = "U"

    age_range_col = None
    for c in ["Age_Range", "Age Range", "AGERANGE"]:
        if c in df.columns:
            age_range_col = c
            break
    if age_range_col:
        df["_AgeRange"] = df[age_range_col].astype(str).str.strip().replace({"nan": "", "None": ""})
    else:
        df["_AgeRange"] = ""

    return df.reset_index(drop=True)

def count_households(frame: pd.DataFrame) -> int:
    if "HH_ID" in frame.columns:
        hh = frame["HH_ID"].astype(str).str.strip()
        hh = hh[hh != ""]
        if not hh.empty:
            return hh.nunique()
    parts = []
    for col in ["House Number", "Street Name", "Apartment Number"]:
        if col in frame.columns:
            parts.append(frame[col].astype(str).fillna(""))
    if not parts:
        return 0
    key = parts[0]
    for p in parts[1:]:
        key = key + "|" + p
    return key.nunique()

def build_area_summary(frame: pd.DataFrame, area_col: str) -> pd.DataFrame:
    temp = frame.copy()
    if "HH_ID" in temp.columns:
        hh = temp["HH_ID"].astype(str).str.strip()
        temp["_hh"] = hh.where(hh != "", temp.index.astype(str))
    else:
        key_parts = []
        for col in ["House Number", "Street Name", "Apartment Number"]:
            if col in temp.columns:
                key_parts.append(temp[col].astype(str).fillna(""))
        if key_parts:
            hh_key = key_parts[0]
            for p in key_parts[1:]:
                hh_key = hh_key + "|" + p
            temp["_hh"] = hh_key
        else:
            temp["_hh"] = temp.index.astype(str)

    out = (
        temp.groupby(area_col, dropna=False)
        .agg(Individuals=(area_col, "size"), Households=("_hh", "nunique"))
        .reset_index()
        .sort_values("Individuals", ascending=False)
        .reset_index(drop=True)
    )
    out[area_col] = out[area_col].astype(object).where(out[area_col].notna(), "(Blank)")
    return out

st.title("Candidate Connect")
st.info("Loading data from Google Drive...")

try:
    df = load_data()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

st.success("Data loaded successfully")

with st.sidebar:
    st.header("Filters")
    filter_cols = [c for c in ["Party", "County", "Municipality", "Precinct"] if c in df.columns]
    selections = {}
    for col in filter_cols:
        vals = df[col].dropna().astype(str).str.strip()
        vals = sorted([v for v in vals.unique().tolist() if v != ""])
        selections[col] = st.multiselect(col, vals)

    age_range = None
    if pd.to_numeric(df["_AgeNum"], errors="coerce").notna().any():
        age_min = int(pd.to_numeric(df["_AgeNum"], errors="coerce").min())
        age_max = int(pd.to_numeric(df["_AgeNum"], errors="coerce").max())
        age_range = st.slider("Age", age_min, age_max, (age_min, age_max))

filtered = df.copy()
for col, picked in selections.items():
    if picked:
        filtered = filtered[filtered[col].astype(str).isin(picked)]

if age_range is not None:
    filtered = filtered[(filtered["_AgeNum"] >= age_range[0]) & (filtered["_AgeNum"] <= age_range[1])]

filtered = filtered.reset_index(drop=True)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Voters", f"{len(filtered):,}")
with c2:
    st.metric("Households", f"{count_households(filtered):,}")
with c3:
    st.metric("Unique Counties", f"{filtered['County'].nunique() if 'County' in filtered.columns else 0:,}")
with c4:
    st.metric("Unique Precincts", f"{filtered['Precinct'].nunique() if 'Precinct' in filtered.columns else 0:,}")

ch1, ch2, ch3 = st.columns(3)

with ch1:
    st.subheader("Party Breakdown")
    if "Party" in filtered.columns:
        st.bar_chart(filtered["Party"].value_counts())
    else:
        st.caption("No Party column")

with ch2:
    st.subheader("Gender Breakdown")
    st.bar_chart(filtered["_Gender"].value_counts())

with ch3:
    st.subheader("Age Range Breakdown")
    age_series = filtered["_AgeRange"].replace("", pd.NA).dropna()
    if len(age_series) > 0:
        st.bar_chart(age_series.value_counts())
    else:
        st.caption("No Age Range column")

st.subheader("Counts by Area")
area_choices = [c for c in ["County", "Municipality", "Precinct"] if c in filtered.columns]
if area_choices:
    selected_area = st.selectbox("Area", area_choices, label_visibility="collapsed")
    area_df = build_area_summary(filtered, selected_area).copy()
    for col in ["Individuals", "Households"]:
        area_df[col] = pd.to_numeric(area_df[col], errors="coerce").fillna(0).astype(int)
    st.dataframe(area_df, use_container_width=True, hide_index=True)
else:
    st.caption("No area columns found")

st.subheader("Preview")
st.dataframe(filtered.head(100), use_container_width=True, hide_index=True)
