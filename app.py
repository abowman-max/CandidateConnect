
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from datetime import datetime
import base64
import json
import re
import shutil
from typing import Any
from html import escape

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Candidate Connect", layout="wide", initial_sidebar_state="expanded")

BASE = Path(__file__).parent
CSV_PATH = BASE / "data.csv"
PARQUET_PATH = BASE / "data.parquet"
FILTER_JSON_PATH = BASE / "filter_options.json"
LOCAL_VERSION_PATH = BASE / "version.txt"
CC_LOGO = BASE / "candidate_connect_logo.png"
TSS_LOGO = BASE / "TSS_Logo_Transparent.png"
FINISHED_REPORTS_DIR = BASE / "Finished Reports"

GDRIVE_DATA_FILE_ID = "102YgV6ev74B_FutPsO1SIwUcyjZtETWa"
GDRIVE_VERSION_FILE_ID = "102YgV6ev74B_FutPsO1SIwUcyjZtETWa"

st.markdown(
    """
    <style>
    .block-container {padding-top: .9rem; padding-bottom: .55rem; max-width: 1660px;}
    .top-shell {
        border:1px solid #d8cdcd; border-radius:12px; background:white;
        padding: .7rem 1rem .6rem 1rem; margin-bottom: .7rem;
        box-shadow: 0 1px 2px rgba(0,0,0,.04);
    }
    .filters-card, .counts-card, .chart-card, .table-card {
        border:1px solid #ded7d7; border-radius:10px; background:white;
        padding:.5rem .65rem .55rem .65rem; margin-bottom:.55rem;
        box-shadow: 0 1px 2px rgba(0,0,0,.03);
    }
    .filters-card {background:#f7f5f5;}
    .filter-section {
        border:1px solid #cfc4c4; border-radius:8px; background:#ffffff; margin-bottom:.38rem; overflow:hidden;
    }
    .filter-section-title {
        font-size:11px; font-weight:700; color:#ffffff;
        background:#7a1523; padding:.44rem .65rem;
    }
    .filter-section-body {padding:.42rem .5rem .2rem .5rem;}
    .stMultiSelect [data-baseweb="select"] > div, .stSelectbox [data-baseweb="select"] > div {
        min-height:27px; font-size:.73rem;
    }
    .stSelectbox div[data-baseweb="select"] span,
    .stMultiSelect div[data-baseweb="select"] span,
    .stMarkdown, .stCaption, p, li, label, div[data-testid="stMetricLabel"] {
        font-size:11px !important;
    }
    .stSlider p, .stSlider label, .stSlider div {font-size:11px !important; color:#2b2b2b !important; font-weight:600;}
    .filter-note {font-size:10px; color:#5a5a5a; margin:.05rem 0 .25rem 0;}
    .contact-label {font-size:11px; font-weight:600; color:#333; margin:.18rem 0 .08rem 0;}
    label[data-testid="stWidgetLabel"] {display:none !important;}
    .stButton > button, .stDownloadButton > button, div[data-testid="stFormSubmitButton"] button {
        background:#7a1523; color:white; border:1px solid #5b0f19; border-radius:7px;
        padding:.14rem .72rem; font-size:11px; font-weight:600; min-height:1.45rem;
        width:100% !important;
    }
    .stButton > button:hover, .stDownloadButton > button:hover, div[data-testid="stFormSubmitButton"] button:hover {
        background:#5d101b; border-color:#4a4c50; color:#f4f4f4;
    }
    div[data-testid="stMetricValue"] {font-size: 1.16rem;}
    div[data-testid="stMetricLabel"] {font-size: 11px;}
    .powered {font-size:11px; color:#666; text-align:center; margin-top:.1rem; margin-bottom:.03rem;}
    .powered-wrap {display:flex; flex-direction:column; align-items:center; justify-content:center;}
    .last-updated {font-size:10px; color:#666; margin-top:-.15rem; margin-bottom:.2rem;}
    h2, h3 {font-size: 1rem !important;}
    .small-header {font-size:12px; font-weight:700; color:#3a3a3a; margin-bottom:.18rem;}
    .tiny-muted {font-size:10px; color:#666;}
    .splash {
        border:1px solid #d8cdcd; border-radius:16px; background:linear-gradient(180deg,#ffffff 0%,#fbf8f8 100%);
        padding: 1.1rem 1rem; margin-bottom: .8rem; text-align:center;
        box-shadow: 0 1px 2px rgba(0,0,0,.04);
    }
    .splash-logo {
        width: 205px; margin: 0 auto .55rem auto; display:block;
        animation: pulse 1.3s ease-in-out infinite;
    }
    .splash-title {font-size:18px; font-weight:700; color:#7a1523; margin-bottom:.2rem;}
    .splash-sub {font-size:11px; color:#666; margin-bottom:.25rem;}
    .status-pill {
        display:inline-block; border:1px solid #d5c4c7; background:#fff; color:#5d101b;
        border-radius:999px; padding:.18rem .55rem; font-size:10px; font-weight:700;
        margin-top:.15rem;
    }
    .report-note {
        font-size:10px; color:#666; padding-top:.1rem; padding-bottom:.15rem;
    }
    .action-card {border:1px solid #ded7d7; border-radius:10px; background:#fcfbfb; padding:.55rem .65rem; min-height:92px; margin-bottom:.45rem;}
    .action-title {font-size:11px; font-weight:700; color:#2f3134; margin-bottom:.12rem;}
    .action-sub {font-size:10px; color:#666; min-height:28px;}
    .files-header {display:grid; grid-template-columns: 2.1fr 1fr 1.2fr .9fr; gap:.5rem; align-items:center; border-bottom:1px solid #dedede; padding:.1rem 0 .35rem 0; font-size:10px; font-weight:700; color:#666; text-transform:uppercase; letter-spacing:.02em;}
    .files-name {font-size:11px; font-weight:700; color:#2f3134;}
    .files-meta {font-size:10px; color:#555; text-align:center;}
    .cc-table {width:100%; border-collapse:collapse; font-size:11px;}
    .cc-table th {background:#7a1523; color:#fff; padding:6px 8px; text-align:center; font-weight:700;}
    .cc-table td {padding:5px 8px; text-align:center; border-bottom:1px solid #ece7e7; color:#2f3134;}
    .ready-pill {display:inline-flex; align-items:center; gap:.35rem; font-size:11px; font-weight:700; color:#246b2c;}
    .ready-dot {width:9px; height:9px; border-radius:50%; background:#2eaf45; display:inline-block;}
    .finished-card {border:1px solid #d9dede; border-radius:10px; background:#fbfdfb; padding:.55rem .65rem; margin-bottom:.45rem;}
    .finished-title {font-size:11px; font-weight:700; color:#2f3134; margin-bottom:.12rem;}
    .finished-meta {font-size:10px; color:#666; margin-bottom:.3rem;}
    .metric-tile {border:1px solid #ded7d7; border-radius:10px; background:white; padding:.45rem .55rem; height:100%;}
    .metric-label {font-size:11px; color:#666; margin-bottom:.1rem;}
    .metric-value {font-size:1.08rem; font-weight:700; color:#2f3134;}
    .stExpander {border:1px solid #cfc4c4; border-radius:8px; background:#ffffff; margin-bottom:.38rem; overflow:hidden;}
    .streamlit-expanderHeader {font-size:11px !important; font-weight:700 !important; color:#ffffff !important; background:#7a1523 !important; padding:.44rem .65rem !important;}
    .streamlit-expanderContent {padding:.42rem .5rem .2rem .5rem !important;}
    div[data-testid="stDataFrame"] [role="row"] {min-height: 26px !important;}
    div[data-testid="stDataFrame"] [role="gridcell"], div[data-testid="stDataFrame"] [role="columnheader"] {padding-top: 2px !important; padding-bottom: 2px !important;}
    .overlay-wrap {
        position: fixed; inset: 0; z-index: 99999;
        display: flex; align-items: center; justify-content: center;
        background: rgba(247,245,245,.72);
        backdrop-filter: blur(1.5px);
        padding: 1rem;
    }
    .overlay-card {
        width: min(440px, 92vw);
        background: #ffffff;
        border: 1px solid #d8cdcd;
        border-radius: 18px;
        box-shadow: 0 14px 38px rgba(0,0,0,.14);
        padding: 1rem 1rem .95rem 1rem;
        text-align: center;
    }
    .overlay-logo {
        width: 190px;
        max-width: 72%;
        margin: 0 auto .55rem auto;
        display: block;
        animation: pulse 1.2s ease-in-out infinite;
    }
    .overlay-title {
        font-size: 18px;
        font-weight: 700;
        color: #7a1523;
        margin-bottom: .18rem;
    }
    .overlay-sub {
        font-size: 11px;
        color: #666;
        margin-bottom: .55rem;
    }
    .overlay-percent {
        font-size: 22px;
        line-height: 1;
        font-weight: 700;
        color: #2f3134;
        margin-bottom: .5rem;
    }
    .overlay-progress-shell {
        width: 100%;
        height: 12px;
        background: #ece7e7;
        border-radius: 999px;
        overflow: hidden;
        border: 1px solid #ddd4d4;
    }
    .overlay-progress-fill {
        height: 100%;
        background: linear-gradient(90deg, #7a1523 0%, #b8454f 100%);
        border-radius: 999px;
        transition: width .18s ease;
    }
    @keyframes pulse {
        0% { transform: scale(1); opacity: .92; }
        50% { transform: scale(1.03); opacity: 1; }
        100% { transform: scale(1); opacity: .92; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

TITLE_WORD_EXCEPTIONS = {
    "GOP": "GOP", "MDJ": "MDJ", "USC": "USC", "STS": "STS", "STH": "STH",
    "PA": "PA", "RD": "Rd", "ST": "St", "AVE": "Ave", "BLVD": "Blvd", "DR": "Dr",
    "LN": "Ln", "PL": "Pl", "CT": "Ct", "CIR": "Cir", "PKWY": "Pkwy", "HWY": "Hwy",
    "APT": "Apt", "N": "N", "S": "S", "E": "E", "W": "W", "NE": "NE", "NW": "NW",
    "SE": "SE", "SW": "SW", "II": "II", "III": "III", "IV": "IV", "JR": "Jr", "SR": "Sr"
}
TAG_LABELS = {
    "TAG0001_Pro2A": "Pro2A",
    "TAG00011_Pro2A_FOAC_TARG": "Pro2A FOAC Targ",
    "TAG0002_MB_Target": "MB Target",
    "TAG0003_New_Reg": "New Reg",
    "TAG0004_ProLife": "ProLife",
    "TAG0005_ProLabor": "ProLabor",
    "TAG0006_RepDonor": "RepDonor",
    "TAG0007_DemDonor": "DemDonor",
    "TAG0008_TrumpDonor": "TrumpDonor",
    "TAG00090_PADonor": "PADonor",
    "TAG00100_FedDonor": "FedDonor",
    "TAG00110_AllDonor": "AllDonor",
    "TAG0014_Teacher": "Teacher",
    "TAG0015_RetiredTeacher": "RetiredTeacher",
}
EXISTENCE_FILTERS = ["Email", "PrimaryPhone", "Landline", "Mobile"]
VM_METHOD_MAP = {"Mail Ballot": "MB", "At Poll": "AP", "Provisional": "P", "Did Not Vote": "DNV"}
VM_TYPE_MAP = {"P": "Primary", "G": "General"}
PARTY_COLOR_MAP = {"R": "#c62828", "D": "#1565c0", "O": "#2e7d32"}
AGE_COLOR_RANGE = ["#7a1523","#9f2032","#b8454f","#c96a6c","#d88f87","#e8b8aa","#f2dbcf","#f7ebe5","#fbf5f2"]
GENDER_COLOR_RANGE = ["#7a1523","#4b4f54","#b98088","#9b9da1","#d8b6bb"]

TITLE_MATCH_COLS = {"County","Municipality","Precinct","School District","CalculatedParty"}
CONTACT_BOOL_MAP = {"Email": "_has_email", "PrimaryPhone": "_has_primaryphone", "Landline": "_has_landline", "Mobile": "_has_mobile"}

def _clean_str(val: Any) -> str:
    if pd.isna(val):
        return ""
    text = str(val).strip()
    return "" if text.lower() in {"nan", "none"} else text

def _build_full_name_columns(df: pd.DataFrame) -> pd.Series:
    if "FullName" in df.columns:
        full = df["FullName"].astype("object").map(smart_title)
        full_text = full.astype(str).str.strip().replace({"nan": "", "None": ""})
        if (full_text != "").any():
            return full_text
    parts = []
    for col in ["FirstName", "MiddleName", "LastName", "NameSuffix"]:
        if col in df.columns:
            parts.append(df[col].astype("object").fillna("").astype(str).str.strip())
        else:
            parts.append(pd.Series([""] * len(df), index=df.index))
    full = (parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]).str.replace(r"\s+", " ", regex=True).str.strip()
    return full.map(smart_title)

def _build_household_key_from_frame(df: pd.DataFrame) -> pd.Series:
    if "HH_ID" in df.columns:
        hh = df["HH_ID"].astype("object").fillna("").astype(str).str.strip()
        if (hh != "").any():
            base_house = df["House Number"].astype("object").fillna("").astype(str) if "House Number" in df.columns else pd.Series([""] * len(df), index=df.index)
            base_street = df["Street Name"].astype("object").fillna("").astype(str) if "Street Name" in df.columns else pd.Series([""] * len(df), index=df.index)
            base_apt = df["Apartment Number"].astype("object").fillna("").astype(str) if "Apartment Number" in df.columns else pd.Series([""] * len(df), index=df.index)
            fallback = base_house + "|" + base_street + "|" + base_apt
            return hh.where(hh != "", fallback)
    base_house = df["House Number"].astype("object").fillna("").astype(str) if "House Number" in df.columns else pd.Series([""] * len(df), index=df.index)
    base_street = df["Street Name"].astype("object").fillna("").astype(str) if "Street Name" in df.columns else pd.Series([""] * len(df), index=df.index)
    base_apt = df["Apartment Number"].astype("object").fillna("").astype(str) if "Apartment Number" in df.columns else pd.Series([""] * len(df), index=df.index)
    return base_house + "|" + base_street + "|" + base_apt

def natural_sort_key(value: Any):
    text = "" if pd.isna(value) else str(value).strip()
    parts = re.split(r"(\d+)", text.lower())
    return [int(p) if p.isdigit() else p for p in parts]

def smart_title(val: object) -> str:
    if pd.isna(val):
        return ""
    text = str(val).strip()
    if not text or text.lower() == "nan":
        return ""
    out = []
    for token in text.replace("_", " ").split():
        upper = token.upper()
        out.append(TITLE_WORD_EXCEPTIONS.get(upper, token.capitalize()))
    return " ".join(out)

def normalize_phone_digits(value: Any) -> str:
    if pd.isna(value):
        return ""
    raw = str(value).strip()
    if not raw or raw.lower() == "nan":
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) >= 10:
        return digits[-10:]
    return ""

def format_phone(value: Any) -> str:
    digits = normalize_phone_digits(value)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:10]}"
    return ""

def is_truthy(value: Any) -> bool:
    if pd.isna(value):
        return False
    return str(value).strip().upper() in {"Y", "YES", "TRUE", "T", "1", "X"}

def value_present(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    return series.notna() & ~text.isin(["", "nan", "None"])

def normalize_vote_method(value: Any) -> str:
    if pd.isna(value):
        return "DNV"
    text = str(value).strip().upper()
    return text if text else "DNV"

def img_to_data_uri(path: Path) -> str:
    if not path.exists():
        return ""
    mime = "image/png"
    encoded = base64.b64encode(path.read_bytes()).decode("utf-8")
    return f"data:{mime};base64,{encoded}"

@st.cache_data(show_spinner=False)
def load_local_version():
    return "Google Drive Web"

@st.cache_data(show_spinner=False)
def read_filter_options_file():
    return None

def fast_data_source() -> str:
    return "google_drive"

def fetch_bytes_url(file_id: str) -> str:
    return f"https://drive.google.com/uc?export=download&id={file_id}"

@st.cache_data(show_spinner=False)
def load_data():
    url = fetch_bytes_url(GDRIVE_DATA_FILE_ID)
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "")
    if "text/html" in content_type.lower() and b"google drive" in response.content[:5000].lower():
        raise RuntimeError("Google Drive returned a web page instead of the parquet file. Make sure the file can be downloaded by link.")
    df = pd.read_parquet(BytesIO(response.content))

    df.columns = [str(c).replace("﻿", "").strip() for c in df.columns]
    status_col = "VoterStatus" if "VoterStatus" in df.columns else ("voterstatus" if "voterstatus" in df.columns else None)
    if status_col:
        status = df[status_col].astype(str).str.strip().str.upper()
        df = df[status == "A"].copy()

    normalize_cols = ["FirstName","MiddleName","LastName","NameSuffix","FullName","Street Name","Municipality","County","Precinct","School District","CalculatedParty"]
    for col in normalize_cols:
        if col in df.columns:
            df[col] = df[col].astype("object").map(smart_title)

    for col in ["Email","PrimaryPhone","Landline","Mobile"]:
        if col in df.columns:
            df[col] = df[col].astype("object").fillna("").astype(str).str.strip()

    for col in ["PrimaryPhone","Landline","Mobile"]:
        if col in df.columns:
            df[col] = df[col].map(normalize_phone_digits)

    for col in TITLE_MATCH_COLS:
        helper = f"_match_{col.lower().replace(' ', '_')}"
        if col in df.columns:
            df[helper] = df[col].astype("object").fillna("").astype(str).str.strip().map(smart_title)

    for col, helper in CONTACT_BOOL_MAP.items():
        if col in df.columns:
            df[helper] = value_present(df[col])

    if "Age" in df.columns:
        df["_age_num"] = pd.to_numeric(df["Age"], errors="coerce")
    if "MB_AProp_Score" in df.columns:
        df["_mb_score_num"] = pd.to_numeric(df["MB_AProp_Score"], errors="coerce")

    df["_household_key"] = _build_household_key_from_frame(df)
    if "FullName" in df.columns:
        df["_full_name_cc"] = df["FullName"].fillna("").astype(str).str.strip().replace({"nan": "", "None": ""}).map(smart_title)
    else:
        name_parts = []
        for col in ["FirstName", "MiddleName", "LastName", "NameSuffix"]:
            if col in df.columns:
                name_parts.append(df[col].fillna("").astype(str).str.strip())
        if name_parts:
            full_name = name_parts[0]
            for part in name_parts[1:]:
                full_name = full_name + " " + part
            df["_full_name_cc"] = full_name.str.replace(r"\s+", " ", regex=True).str.strip().map(smart_title)
        else:
            df["_full_name_cc"] = ""

    return df.reset_index(drop=True)

def get_vm_columns(df: pd.DataFrame):
    vm_cols = {}
    for col in df.columns:
        col_clean = str(col).strip()
        if col_clean.upper().endswith("_VM"):
            try:
                parts = col_clean.split("_")
                prefix = parts[0]
                election_type = prefix[0].upper()
                year = prefix[1:]
                vm_cols[col_clean] = {
                    "type_code": election_type,
                    "type_label": VM_TYPE_MAP.get(election_type, election_type),
                    "year_short": year,
                    "year_label": f"20{year}",
                }
            except Exception:
                continue
    return vm_cols

@st.cache_data(show_spinner=False)
def load_filter_options(df: pd.DataFrame):
    file_options = read_filter_options_file() or {}
    options = {}
    filter_cols = ["Party","County","Municipality","Ward","Precinct","School District","USC","STS","STH","MDJ","V4A","HH-Party","CalculatedParty","MIB_Applied","MIB_BALLOT"]

    fields = file_options.get("fields", file_options)
    for col in filter_cols:
        vals = fields.get(col)
        if vals:
            options[col] = sorted(
                [smart_title(v) if col in {"County","Municipality","Precinct","School District","CalculatedParty"} else str(v) for v in vals],
                key=natural_sort_key,
            )

    for col in filter_cols:
        if col not in options and col in df.columns:
            vals = df[col].dropna().astype(str).str.strip()
            vals = vals[vals != ""]
            if col in {"County","Municipality","Precinct","School District","CalculatedParty"}:
                vals = vals.map(smart_title)
            options[col] = sorted(pd.unique(vals).tolist(), key=natural_sort_key)

    score_range = file_options.get("mb_score_range")
    if score_range:
        options["MB_AProp_Score_range"] = (float(score_range[0]), float(score_range[1]))
    elif "MB_AProp_Score" in df.columns:
        score_vals = pd.to_numeric(df["MB_AProp_Score"], errors="coerce")
        if score_vals.notna().any():
            options["MB_AProp_Score_range"] = (float(score_vals.min()), float(score_vals.max()))

    age_range = file_options.get("age_range")
    if age_range:
        options["age_range"] = (int(age_range[0]), int(age_range[1]))

    options["vm_columns"] = file_options.get("vm_columns") or get_vm_columns(df)
    return options

def _match_series(df: pd.DataFrame, col: str) -> pd.Series:
    helper = f"_match_{col.lower().replace(' ', '_')}"
    if helper in df.columns:
        return df[helper]
    vals = df[col].astype(str).str.strip()
    if col in TITLE_MATCH_COLS:
        vals = vals.map(smart_title)
    return vals

@st.cache_data(show_spinner=False)
def apply_filters(df: pd.DataFrame, selections_json: str):
    selections = json.loads(selections_json)
    mask = pd.Series(True, index=df.index)

    for col, selected in selections.get("multi", {}).items():
        if selected and col in df.columns:
            selected_set = {smart_title(v) if col in TITLE_MATCH_COLS else str(v) for v in selected}
            mask &= _match_series(df, col).isin(selected_set)

    age_range = selections.get("age_range")
    if age_range and "_age_num" in df.columns:
        age_vals = df["_age_num"]
        mask &= age_vals.between(age_range[0], age_range[1], inclusive="both")

    score_range = selections.get("mb_score_range")
    if score_range and "_mb_score_num" in df.columns:
        score_vals = df["_mb_score_num"]
        mask &= score_vals.between(score_range[0], score_range[1], inclusive="both")

    contact_modes = selections.get("exists", {})
    if contact_modes:
        masks = []
        for col, mode in contact_modes.items():
            helper = CONTACT_BOOL_MAP.get(col)
            if helper in df.columns and mode != "Any":
                present = df[helper]
                masks.append(present if mode == "Exists" else ~present)
        if masks:
            combine_mode = selections.get("exists_mode", "All")
            final_mask = masks[0].copy()
            for contact_mask in masks[1:]:
                final_mask = (final_mask & contact_mask) if combine_mode == "All" else (final_mask | contact_mask)
            mask &= final_mask

    selected_tags = selections.get("selected_tags", [])
    tag_mode = selections.get("tag_mode", "Any")
    if selected_tags:
        available_tags = [col for col in selected_tags if col in df.columns]
        if available_tags:
            bool_frame = pd.DataFrame({col: df[col].map(is_truthy) for col in available_tags})
            tag_mask = bool_frame.all(axis=1) if tag_mode == "All" else bool_frame.any(axis=1)
            mask &= tag_mask

    vm_choice = selections.get("vote_method")
    vm_columns_meta = selections.get("vm_columns_meta", {})
    if vm_choice:
        selected_types = vm_choice.get("types", [])
        selected_years = vm_choice.get("years", [])
        selected_methods = vm_choice.get("methods", [])
        if selected_types and selected_years and selected_methods:
            matching_cols = []
            for col, meta in vm_columns_meta.items():
                if meta["type_label"] in selected_types and meta["year_label"] in selected_years and col in df.columns:
                    matching_cols.append(f"_norm_{col}" if f"_norm_{col}" in df.columns else col)
            if matching_cols:
                method_codes = {VM_METHOD_MAP[m] for m in selected_methods}
                vm_match = pd.Series(False, index=df.index)
                for col in matching_cols:
                    series = df[col] if col in df.columns else pd.Series("", index=df.index)
                    vm_match |= series.isin(method_codes)
                mask &= vm_match
            else:
                mask &= False

    return df.loc[mask].reset_index(drop=True)

@st.cache_data(show_spinner=False)
def household_count(frame: pd.DataFrame) -> int:
    if "_household_key" in frame.columns:
        return frame["_household_key"].nunique(dropna=True)
    return _build_household_key_from_frame(frame).nunique(dropna=True)

def household_key(frame: pd.DataFrame) -> pd.Series:
    if "_household_key" in frame.columns:
        return frame["_household_key"]
    return _build_household_key_from_frame(frame)

@st.cache_data(show_spinner=False)
def area_summary(filtered: pd.DataFrame, area_col: str):
    temp = filtered[[area_col, "_household_key"]].copy() if "_household_key" in filtered.columns else filtered[[area_col]].copy()
    if "_household_key" not in temp.columns:
        temp["_household_key"] = household_key(filtered)
    out = (
        temp.groupby(area_col, dropna=False)
        .agg(Individuals=(area_col, "size"), Households=("_household_key", "nunique"))
        .reset_index()
        .sort_values("Individuals", ascending=False)
        .reset_index(drop=True)
    )
    out[area_col] = out[area_col].astype(object).where(out[area_col].notna(), "(Blank)")
    if area_col in TITLE_MATCH_COLS:
        out[area_col] = out[area_col].map(smart_title)
    return out

@st.cache_data(show_spinner=False)
def party_summary(filtered: pd.DataFrame):
    if "Party" not in filtered.columns:
        return pd.DataFrame(columns=["Party","Individuals"])
    out = filtered.groupby("Party", dropna=False).size().reset_index(name="Individuals")
    out["Party"] = out["Party"].astype(object).where(out["Party"].notna(), "O")
    out["Party"] = out["Party"].astype(str).str.strip().replace({"": "O", "None": "O", "nan": "O"})
    out["Party"] = out["Party"].replace({"U": "O"})
    out = out.groupby("Party", dropna=False, as_index=False)["Individuals"].sum()
    return out.sort_values("Individuals", ascending=False).reset_index(drop=True)

@st.cache_data(show_spinner=False)
def gender_summary(filtered: pd.DataFrame):
    col = "Gender" if "Gender" in filtered.columns else ("Sex" if "Sex" in filtered.columns else None)
    if not col:
        return pd.DataFrame(columns=["Gender", "Count"])
    normalized = filtered[col].astype(object).where(filtered[col].notna(), "U")
    normalized = normalized.astype(str).str.strip().replace({"": "U", "None": "U", "nan": "U"})
    normalized = normalized.str.upper().replace({"UNKNOWN": "U", "UN": "U"})
    out = normalized.to_frame(name="Gender").groupby("Gender", dropna=False).size().reset_index(name="Count")
    return out.sort_values("Count", ascending=False).reset_index(drop=True)

@st.cache_data(show_spinner=False)

def age_band_summary(filtered: pd.DataFrame):
    age_range_col = find_first_existing_column(filtered, ["Age Range", "AgeRange", "AGE_RANGE"])
    if age_range_col:
        series = filtered[age_range_col].astype("object").where(filtered[age_range_col].notna(), "")
        series = series.astype(str).str.strip()
        series = series[series != ""]
        if not series.empty:
            out = series.value_counts(dropna=False).rename_axis("Age Range").reset_index(name="Count")
            out["Age Range"] = out["Age Range"].astype(str)
            return out.reset_index(drop=True)

    if "Age" not in filtered.columns:
        return pd.DataFrame(columns=["Age Range", "Count"])
    ages = pd.to_numeric(filtered["Age"], errors="coerce")
    ages = ages[ages.notna()]
    bins = [0, 18, 25, 35, 45, 55, 65, 75, 85, 200]
    labels = ["0-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75-84", "85+"]
    bands = pd.cut(ages, bins=bins, labels=labels, right=False, include_lowest=True)
    out = bands.value_counts(sort=False, dropna=False).reset_index()
    out.columns = ["Age Range", "Count"]
    out["Age Range"] = out["Age Range"].astype(str).replace("nan", "(Blank)")
    out = out[out["Age Range"] != "(Blank)"]
    return out[out["Count"] > 0].reset_index(drop=True)

def largest_area_label(selections: dict[str, Any], filtered: pd.DataFrame) -> str:
    multi = selections.get("multi", {})
    for key in ["County","STH","STS","USC","School District","Municipality","Ward","Precinct"]:
        vals = multi.get(key, [])
        if vals:
            return smart_title(vals[0]) if len(vals) == 1 else f"{len(vals)} {key} selections"
    for key in ["County","Municipality","Precinct"]:
        if key in filtered.columns and filtered[key].nunique(dropna=True) == 1:
            return smart_title(filtered[key].dropna().astype(str).iloc[0])
    return "Selected Area"

def selected_filter_summary(selections: dict[str, Any]) -> str:
    parts = []
    multi = selections.get("multi", {})
    for key in ["Party","County","Municipality","Ward","Precinct","School District","USC","STS","STH","MDJ","V4A","HH-Party","CalculatedParty"]:
        vals = multi.get(key, [])
        if vals:
            value = smart_title(vals[0]) if len(vals) == 1 else f"{len(vals)} {key} selections"
            parts.append(value)
    if selections.get("selected_tags"):
        parts.append("Tagged universe")
    return ", ".join(parts) if parts else "Filtered voter contact list"

def prepare_csv_export(frame: pd.DataFrame) -> pd.DataFrame:
    export_df = frame.copy()
    for col in ["PrimaryPhone", "Landline", "Mobile"]:
        if col in export_df.columns:
            export_df[col] = export_df[col].map(format_phone)
    return export_df

def count_present_values(frame: pd.DataFrame, col: str) -> int:
    helper = CONTACT_BOOL_MAP.get(col)
    if helper in frame.columns:
        return int(frame[helper].sum())
    if col not in frame.columns:
        return 0
    return int(value_present(frame[col]).sum())


def resolve_voter_id_column(frame: pd.DataFrame) -> str | None:
    priority_names = [
        "PA ID Number", "PA_ID_Number", "PAIDNumber", "State Voter ID", "StateVoterID", "STATE_VOTER_ID", "Voter ID", "VoterID",
        "VOTERID", "Voter_ID", "LALVOTERID", "PA Voter ID", "SOS_VOTERID"
    ]
    for col in priority_names:
        if col in frame.columns:
            return col

    lowered = {str(c).strip().lower(): c for c in frame.columns}
    for key in [
        "pa id number", "paidnumber", "pa_id_number", "pa id", "pa voter id",
        "state voter id", "statevoterid", "state_voter_id", "voter id", "voterid",
        "voter_id", "lalvoterid", "sos_voterid"
    ]:
        if key in lowered:
            return lowered[key]

    scored: list[tuple[int, str]] = []
    for col in frame.columns:
        name = str(col).strip().lower()
        score = 0
        if "voter" in name and "id" in name:
            score += 10
        if "state" in name and "id" in name:
            score += 6
        if "voter" in name:
            score += 3
        if name.endswith("id") or "_id" in name or " id" in name:
            score += 2
        if score > 0:
            scored.append((score, col))
    if scored:
        scored.sort(key=lambda item: (-item[0], len(str(item[1]))))
        return scored[0][1]
    return None

def build_texting_export(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "Mobile" not in out.columns:
        return pd.DataFrame(columns=["VoterID", "Mobile", "County", "Precinct"])
    out["Mobile"] = out["Mobile"].map(normalize_phone_digits)
    out = out[out["Mobile"].astype(str).str.strip() != ""].copy()

    voter_id_col = resolve_voter_id_column(frame)
    if voter_id_col and voter_id_col in out.columns:
        out["VoterID"] = out[voter_id_col].astype("object").fillna("").astype(str).str.strip()
    else:
        out["VoterID"] = ""

    for col in ["County", "Precinct"]:
        if col not in out.columns:
            out[col] = ""

    export_df = out[["VoterID", "Mobile", "County", "Precinct"]].copy()
    export_df["County"] = export_df["County"].map(smart_title)
    export_df["Precinct"] = export_df["Precinct"].map(smart_title)
    return export_df.reset_index(drop=True)

def ensure_finished_reports_dir() -> Path:
    return BASE

def save_finished_download(label: str, filename: str, data: bytes, mime: str, kind: str):
    st.session_state.finished_downloads[label] = {
        "filename": filename,
        "data": data,
        "mime": mime,
        "kind": kind,
        "created_at": datetime.now().strftime("%m/%d/%Y %I:%M %p"),
        "size_bytes": int(len(data)),
    }

def save_copy_to_downloads(source_path: str) -> str:
    return ""

def format_file_size(num_bytes: int) -> str:
    size = float(max(num_bytes, 0))
    units = ["B", "KB", "MB", "GB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:,.0f} {unit}" if unit == "B" else f"{size:,.1f} {unit}"
        size /= 1024
    return f"{num_bytes} B"

@st.cache_data(show_spinner=False)
def load_finished_downloads_from_disk(folder_path: str):
    return {}

def sync_finished_downloads_from_disk(force: bool = False):
    if force or not st.session_state.get("finished_downloads_loaded"):
        st.session_state.finished_downloads = st.session_state.get("finished_downloads", {})
        st.session_state.finished_downloads_loaded = True

def clear_finished_downloads_folder():
    st.session_state.finished_downloads = {}
    st.session_state.saved_rows = {}
    st.session_state.finished_downloads_loaded = True

def find_first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in frame.columns:
            return col
    lowered = {str(c).strip().lower(): c for c in frame.columns}
    for col in candidates:
        key = col.strip().lower()
        if key in lowered:
            return lowered[key]
    return None

def _mailing_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "_full_name_cc" in out.columns:
        out["FullName"] = out["_full_name_cc"]
    else:
        out["FullName"] = _build_full_name_columns(out)
    for _col in ["FirstName", "MiddleName", "LastName", "NameSuffix"]:
        if _col not in out.columns:
            out[_col] = ""
    addr1_col = find_first_existing_column(out, ["MailingAddress1", "Mailing Address 1", "MailAddress1", "Address1"])
    addr2_col = find_first_existing_column(out, ["MailingAddress2", "Mailing Address 2", "MailAddress2", "Address2"])
    city_col = find_first_existing_column(out, ["MailingCity", "Mailing City", "City"])
    state_col = find_first_existing_column(out, ["MailingState", "Mailing State", "State"])
    zip_col = find_first_existing_column(out, ["MailingZip", "Mailing Zip", "Zip", "ZipCode", "ZIPCODE"])

    if not addr1_col:
        house = out["House Number"].map(_clean_str) if "House Number" in out.columns else pd.Series([""] * len(out), index=out.index)
        street = out["Street Name"].map(_clean_str).map(smart_title) if "Street Name" in out.columns else pd.Series([""] * len(out), index=out.index)
        out["MailingAddress1"] = (house + " " + street).str.replace(r"\s+", " ", regex=True).str.strip()
    else:
        out["MailingAddress1"] = out[addr1_col].map(_clean_str)

    if not addr2_col:
        if "Apartment Number" in out.columns:
            out["MailingAddress2"] = out["Apartment Number"].map(_clean_str).map(lambda x: f"Apt {smart_title(x)}" if x else "")
        else:
            out["MailingAddress2"] = ""
    else:
        out["MailingAddress2"] = out[addr2_col].map(_clean_str)

    out["MailingCity"] = out[city_col].map(_clean_str).map(smart_title) if city_col else (out["Municipality"].map(_clean_str).map(smart_title) if "Municipality" in out.columns else "")
    out["MailingState"] = out[state_col].map(_clean_str) if state_col else ""
    out["MailingZip"] = out[zip_col].map(_clean_str) if zip_col else ""

    for col in ["Precinct", "Municipality", "County"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(_clean_str).map(smart_title)

    out["_household_key"] = household_key(out)
    return out

def _join_names_for_envelope(names: list[str]) -> str:
    names = [n for n in names if n]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} & {names[1]}"
    if len(names) == 3:
        return f"{names[0]}, {names[1]} & {names[2]}"
    return f"{names[0]}, {names[1]} & {len(names) - 2} Others"

def _household_mailing_name(group: pd.DataFrame) -> str:
    names = [n for n in group["FullName"].astype(str).str.strip().tolist() if n]
    unique_names = list(dict.fromkeys(names))
    if len(unique_names) <= 1:
        return unique_names[0] if unique_names else ""

    last_names = [ln for ln in group["LastName"].astype(str).str.strip().tolist() if ln]
    unique_last = list(dict.fromkeys(last_names))
    if len(unique_last) == 1 and unique_last[0]:
        return f"{smart_title(unique_last[0])} Household"

    return _join_names_for_envelope(unique_names[:4])

def build_mail_export(frame: pd.DataFrame, householded: bool = False) -> pd.DataFrame:
    out = _mailing_columns(frame)
    base_cols = [
        "MailingName", "FullName", "FirstName", "MiddleName", "LastName", "NameSuffix",
        "MailingAddress1", "MailingAddress2", "MailingCity", "MailingState", "MailingZip",
        "Precinct", "Municipality", "County", "Individuals"
    ]

    if not householded:
        out["MailingName"] = out["FullName"]
        out["Individuals"] = 1
        result = out[base_cols].copy()
        return result.reset_index(drop=True)

    grouped_rows = []
    for _, group in out.groupby("_household_key", dropna=False, sort=False):
        first = group.iloc[0]
        grouped_rows.append({
            "MailingName": _household_mailing_name(group),
            "FullName": _join_names_for_envelope([n for n in group["FullName"].astype(str).str.strip().tolist() if n][:4]),
            "FirstName": "",
            "MiddleName": "",
            "LastName": "",
            "NameSuffix": "",
            "MailingAddress1": _clean_str(first.get("MailingAddress1", "")),
            "MailingAddress2": _clean_str(first.get("MailingAddress2", "")),
            "MailingCity": smart_title(first.get("MailingCity", "")),
            "MailingState": _clean_str(first.get("MailingState", "")),
            "MailingZip": _clean_str(first.get("MailingZip", "")),
            "Precinct": smart_title(first.get("Precinct", "")),
            "Municipality": smart_title(first.get("Municipality", "")),
            "County": smart_title(first.get("County", "")),
            "Individuals": int(len(group)),
        })
    result = pd.DataFrame(grouped_rows, columns=base_cols)
    return result.reset_index(drop=True)

def build_household_export(frame: pd.DataFrame) -> pd.DataFrame:
    return build_mail_export(frame, householded=True)

def build_phone_bank_export(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "_full_name_cc" in out.columns:
        out["FullName"] = out["_full_name_cc"]
    elif "FullName" not in out.columns:
        name_cols = [c for c in ["FirstName","MiddleName","LastName","NameSuffix"] if c in out.columns]
        if name_cols:
            out["FullName"] = out[name_cols].fillna("").astype(str).agg(" ".join, axis=1).str.replace(r"\s+", " ", regex=True).str.strip()
        else:
            out["FullName"] = ""

    house = out["House Number"].astype(str).fillna("") if "House Number" in out.columns else pd.Series([""] * len(out), index=out.index)
    street = out["Street Name"].astype(str).fillna("") if "Street Name" in out.columns else pd.Series([""] * len(out), index=out.index)
    out["AddressLine1"] = (house + " " + street).str.replace(r"\s+", " ", regex=True).str.strip()
    if "Apartment Number" in out.columns:
        out["AddressLine2"] = out["Apartment Number"].astype(str).fillna("").replace({"nan": "", "None": ""}).map(lambda x: f"Apt {x}" if str(x).strip() else "")
    else:
        out["AddressLine2"] = ""

    for col in ["Landline", "Mobile"]:
        if col in out.columns:
            out[col] = out[col].map(format_phone)
        else:
            out[col] = ""

    keep = [c for c in ["FullName", "FirstName", "MiddleName", "LastName", "NameSuffix", "AddressLine1", "AddressLine2", "Landline", "Mobile", "Municipality", "Precinct", "County"] if c in out.columns]
    export_df = out[keep].copy()
    landline_series = export_df["Landline"].astype(str).str.strip() if "Landline" in export_df.columns else pd.Series([""] * len(export_df), index=export_df.index)
    mobile_series = export_df["Mobile"].astype(str).str.strip() if "Mobile" in export_df.columns else pd.Series([""] * len(export_df), index=export_df.index)
    export_df = export_df[(landline_series != "") | (mobile_series != "")].reset_index(drop=True)
    for col in ["Municipality", "Precinct", "County"]:
        if col in export_df.columns:
            export_df[col] = export_df[col].map(smart_title)
    return export_df

def build_area_counts_export(frame: pd.DataFrame, area_col: str) -> pd.DataFrame:
    return area_summary(frame, area_col)

def build_excel_export(filtered: pd.DataFrame, area_col: str, selections: dict[str, Any], householded: bool = False) -> bytes:
    prepared = prepare_csv_export(filtered)
    mail_file = build_mail_export(filtered, householded=householded)
    phone_bank = build_phone_bank_export(filtered)
    counts = build_area_counts_export(filtered, area_col)
    party = party_summary(filtered)
    age = age_band_summary(filtered)
    gender = gender_summary(filtered)

    overview = pd.DataFrame(
        [
            ["Report Area", largest_area_label(selections, filtered)],
            ["Filter Summary", selected_filter_summary(selections)],
            ["Voters", len(filtered)],
            ["Households", household_count(filtered)],
            ["Generated From", fast_data_source()],
            ["Data Version", load_local_version()],
        ],
        columns=["Field", "Value"],
    )

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        overview.to_excel(writer, index=False, sheet_name="Overview")
        prepared.to_excel(writer, index=False, sheet_name="Filtered Voters")
        mail_file.to_excel(writer, index=False, sheet_name="Householded Mail" if householded else "Mail File")
        phone_bank.to_excel(writer, index=False, sheet_name="Phone Bank")
        counts.to_excel(writer, index=False, sheet_name="Counts By Area")
        party.to_excel(writer, index=False, sheet_name="Party Summary")
        age.to_excel(writer, index=False, sheet_name="Age Summary")
        gender.to_excel(writer, index=False, sheet_name="Gender Summary")
    return buffer.getvalue()


def render_compact_table(df_in: pd.DataFrame):
    if df_in.empty:
        st.caption("No data")
        return
    headers = "".join([f"<th>{escape(str(col))}</th>" for col in df_in.columns])
    rows = []
    for _, row in df_in.iterrows():
        cells = "".join([f"<td>{escape(str(val))}</td>" for val in row.tolist()])
        rows.append(f"<tr>{cells}</tr>")
    st.markdown(f"<table class='cc-table'><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>", unsafe_allow_html=True)

def pie_chart_with_table(df_chart: pd.DataFrame, label_col: str, value_col: str, title: str, color_mode: str = "default"):
    st.markdown(f'<div class="small-header">{title}</div>', unsafe_allow_html=True)
    if df_chart.empty:
        st.caption("No data")
        return
    chart_df = df_chart.copy()
    domain = chart_df[label_col].astype(str).tolist()
    if color_mode == "party":
        colors = [PARTY_COLOR_MAP.get(v, "#757575") for v in domain]
    elif color_mode == "age":
        colors = AGE_COLOR_RANGE[:len(domain)]
    else:
        colors = GENDER_COLOR_RANGE[:len(domain)]
    chart = alt.Chart(chart_df).mark_arc(innerRadius=18, outerRadius=58).encode(
        theta=alt.Theta(field=value_col, type="quantitative"),
        color=alt.Color(field=label_col, type="nominal", scale=alt.Scale(domain=domain, range=colors), legend=alt.Legend(orient="bottom", title=None, labelFontSize=10)),
        tooltip=[alt.Tooltip(f"{label_col}:N"), alt.Tooltip(f"{value_col}:Q", format=",")]
    ).properties(height=210)
    st.altair_chart(chart, use_container_width=True)
    display_df = chart_df[[label_col, value_col]].copy()
    display_df[value_col] = pd.to_numeric(display_df[value_col], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
    render_compact_table(display_df)

def get_file_modified_text():
    return "Google Drive data source"

def fetch_text_url(file_id: str) -> str:
    return f"https://drive.google.com/uc?export=download&id={file_id}"


# Initial data load
logo_uri = img_to_data_uri(CC_LOGO)
overlay = st.empty()

def render_startup_overlay(percent: int, message: str):
    percent = max(0, min(100, int(percent)))
    logo_html = f'<img class="overlay-logo" src="{logo_uri}"/>' if logo_uri else ''
    overlay.markdown(f"""
        <div class="overlay-wrap">
          <div class="overlay-card">
            {logo_html}
            <div class="overlay-title">Candidate Connect</div>
            <div class="overlay-sub">{escape(message)}</div>
            <div class="overlay-percent">{percent}%</div>
            <div class="overlay-progress-shell"><div class="overlay-progress-fill" style="width:{percent}%;"></div></div>
          </div>
        </div>
    """, unsafe_allow_html=True)

def startup_step(percent: int, message: str):
    render_startup_overlay(percent, message)

startup_step(5, "Starting Candidate Connect…")
source_note_for_overlay = "Parquet data file" if fast_data_source() == "parquet" else "CSV data file"
startup_step(12, f"Opening {source_note_for_overlay}…")
df = load_data()
if df is None:
    overlay.empty()
    st.error("I could not find data.csv or data.parquet in this folder.")
    st.stop()
startup_step(68, "Loading saved filter options…")
options = load_filter_options(df)
startup_step(82, "Checking saved files…")
ensure_finished_reports_dir()
startup_step(92, "Finalizing dashboard…")
age_bounds = options.get("age_range")
if not age_bounds and "_age_num" in df.columns:
    age_num = df["_age_num"]
    if age_num.notna().any():
        age_bounds = (int(age_num.min()), int(age_num.max()))
score_bounds = options.get("MB_AProp_Score_range")
startup_step(100, "Ready")
overlay.empty()

if "finished_downloads" not in st.session_state:
    st.session_state.finished_downloads = {}
if "saved_rows" not in st.session_state:
    st.session_state.saved_rows = {}
if "finished_downloads_loaded" not in st.session_state:
    st.session_state.finished_downloads_loaded = False
sync_finished_downloads_from_disk()

if "filter_state" not in st.session_state:
    st.session_state.filter_state = {
        "multi": {},
        "age_range": age_bounds,
        "mb_score_range": score_bounds,
        "exists": {col: "Any" for col in EXISTENCE_FILTERS if col in df.columns},
        "exists_mode": "All",
        "selected_tags": [],
        "tag_mode": "Any",
        "vote_method": {"types": [], "years": [], "methods": []},
        "vm_columns_meta": options.get("vm_columns", {}),
    }

if "mailing_mode" not in st.session_state:
    st.session_state.mailing_mode = "Not Householded"

st.markdown('<div class="top-shell">', unsafe_allow_html=True)
head_left, head_mid, head_right = st.columns([2.1, 4.0, 1.2], vertical_alignment="center")
with head_left:
    if CC_LOGO.exists():
        st.image(str(CC_LOGO), width=155)
    else:
        st.markdown("## Candidate Connect")
with head_mid:
    st.markdown('<div class="small-header">Candidate Connect</div>', unsafe_allow_html=True)
    st.markdown('<div class="tiny-muted">Campaign data, counts, exports, and street lists</div>', unsafe_allow_html=True)
    modified = get_file_modified_text()
    local_ver = load_local_version()
    source_note = "Google Drive Parquet"
    if modified:
        st.markdown(f'<div class="last-updated">Last Updated: {modified} &nbsp;&nbsp;|&nbsp;&nbsp; Data Version: {local_ver} &nbsp;&nbsp;|&nbsp;&nbsp; Source: {source_note}</div>', unsafe_allow_html=True)
with head_right:
    st.markdown('<div class="powered-wrap"><div class="powered">Powered By</div></div>', unsafe_allow_html=True)
    if TSS_LOGO.exists():
        _, center_logo_col, _ = st.columns([1, 1.2, 1])
        with center_logo_col:
            st.image(str(TSS_LOGO), width=68)
st.markdown('</div>', unsafe_allow_html=True)

left_col, right_col = st.columns([1.08, 2.92], gap="large")

with left_col:
    st.markdown('<div class="filters-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-header">Filters</div>', unsafe_allow_html=True)
    with st.form("filters_form", clear_on_submit=False):
        new_state = {
            "multi": {},
            "age_range": st.session_state.filter_state.get("age_range", age_bounds),
            "mb_score_range": st.session_state.filter_state.get("mb_score_range", score_bounds),
            "exists": st.session_state.filter_state.get("exists", {}).copy(),
            "exists_mode": st.session_state.filter_state.get("exists_mode", "All"),
            "selected_tags": st.session_state.filter_state.get("selected_tags", []).copy(),
            "tag_mode": st.session_state.filter_state.get("tag_mode", "Any"),
            "vote_method": st.session_state.filter_state.get("vote_method", {"types": [], "years": [], "methods": []}).copy(),
            "vm_columns_meta": options.get("vm_columns", {}),
        }

        with st.expander("▼ Area", expanded=False):
            core_cols = ["Party","County","Municipality","Ward","Precinct","School District","USC","STS","STH","MDJ"]
            for col in core_cols:
                if col in options:
                    new_state["multi"][col] = st.multiselect(col, options[col], default=st.session_state.filter_state.get("multi", {}).get(col, []), placeholder=col, label_visibility="collapsed")

        with st.expander("▼ Voter / Household", expanded=False):
            extra_cols = ["V4A","HH-Party","CalculatedParty","MIB_Applied","MIB_BALLOT"]
            for col in extra_cols:
                if col in options:
                    new_state["multi"][col] = st.multiselect(col, options[col], default=st.session_state.filter_state.get("multi", {}).get(col, []), placeholder=col, label_visibility="collapsed")
            if age_bounds:
                st.markdown('<div class="contact-label">Age</div>', unsafe_allow_html=True)
                new_state["age_range"] = st.slider("Age", age_bounds[0], age_bounds[1], st.session_state.filter_state.get("age_range", age_bounds), label_visibility="collapsed")
            if score_bounds:
                st.markdown('<div class="contact-label">Mail Ballot Probability</div>', unsafe_allow_html=True)
                new_state["mb_score_range"] = st.slider("Mail Ballot Probability", float(score_bounds[0]), float(score_bounds[1]), st.session_state.filter_state.get("mb_score_range", score_bounds), label_visibility="collapsed")

        with st.expander("▼ Contact Presence", expanded=False):
            st.markdown('<div class="filter-note">Set each contact type to Exists or Does Not Exist. Use Match Rule to require All selected rules or Any selected rule.</div>', unsafe_allow_html=True)
            pretty_names = {"Email": "Email", "Mobile": "Mobile", "Landline": "Landline", "PrimaryPhone": "Primary Phone"}
            for col in ["Email", "Mobile", "Landline", "PrimaryPhone"]:
                if col in df.columns:
                    st.markdown(f'<div class="contact-label">{pretty_names[col]}</div>', unsafe_allow_html=True)
                    current_val = st.session_state.filter_state.get("exists", {}).get(col, "Any")
                    new_state["exists"][col] = st.selectbox(pretty_names[col], ["Any", "Exists", "Does Not Exist"], index=["Any", "Exists", "Does Not Exist"].index(current_val), label_visibility="collapsed")
            st.markdown('<div class="contact-label">Match Rule</div>', unsafe_allow_html=True)
            new_state["exists_mode"] = st.selectbox("Match Rule", ["All", "Any"], index=["All", "Any"].index(st.session_state.filter_state.get("exists_mode", "All")), label_visibility="collapsed")

        available_tags = [col for col in TAG_LABELS if col in df.columns]
        if available_tags:
            with st.expander("▼ Tags", expanded=False):
                tag_labels = [TAG_LABELS[col] for col in available_tags]
                current = [TAG_LABELS[col] for col in st.session_state.filter_state.get("selected_tags", []) if col in TAG_LABELS and col in available_tags]
                picked_labels = st.multiselect("Tags", tag_labels, default=current, placeholder="Tags", label_visibility="collapsed")
                reverse_map = {v: k for k, v in TAG_LABELS.items()}
                new_state["selected_tags"] = [reverse_map[label] for label in picked_labels]
                new_state["tag_mode"] = st.selectbox("Tag Match", ["Any", "All"], index=["Any", "All"].index(st.session_state.filter_state.get("tag_mode", "Any")), label_visibility="collapsed")

        vm_meta = options.get("vm_columns", {}) or get_vm_columns(df)
        if vm_meta:
            with st.expander("▼ Vote Method", expanded=False):
                vm_types = sorted({meta["type_label"] for meta in vm_meta.values()}, key=natural_sort_key)
                vm_years = sorted({meta["year_label"] for meta in vm_meta.values()}, key=natural_sort_key)
                new_state["vote_method"]["types"] = st.multiselect("Election Type", vm_types, default=st.session_state.filter_state.get("vote_method", {}).get("types", []), placeholder="Election Type", label_visibility="collapsed")
                new_state["vote_method"]["years"] = st.multiselect("Election Year", vm_years, default=st.session_state.filter_state.get("vote_method", {}).get("years", []), placeholder="Election Year", label_visibility="collapsed")
                new_state["vote_method"]["methods"] = st.multiselect("Vote Method", list(VM_METHOD_MAP.keys()), default=st.session_state.filter_state.get("vote_method", {}).get("methods", []), placeholder="Vote Method", label_visibility="collapsed")

        btn1, btn2 = st.columns([1,1])
        with btn1:
            apply_clicked = st.form_submit_button("Apply Filters")
        with btn2:
            clear_clicked = st.form_submit_button("Clear Filters")
    st.markdown('</div>', unsafe_allow_html=True)

if clear_clicked:
    st.session_state.filter_state = {
        "multi": {},
        "age_range": age_bounds,
        "mb_score_range": score_bounds,
        "exists": {col: "Any" for col in EXISTENCE_FILTERS if col in df.columns},
        "exists_mode": "All",
        "selected_tags": [],
        "tag_mode": "Any",
        "vote_method": {"types": [], "years": [], "methods": []},
        "vm_columns_meta": options.get("vm_columns", {}),
    }
    st.rerun()

if apply_clicked:
    st.session_state.filter_state = new_state

selection_payload = json.dumps(st.session_state.filter_state, sort_keys=True)
filtered = apply_filters(df, selection_payload)

with right_col:
    st.markdown('<div class="counts-card">', unsafe_allow_html=True)
    metric_cols = st.columns([1, 1, 1, 1, 1, 1.8], gap="small")
    metric_values = [
        ("Voters", f"{len(filtered):,}"),
        ("Households", f"{household_count(filtered):,}"),
        ("Emails", f"{count_present_values(filtered, 'Email'):,}"),
        ("Landlines", f"{count_present_values(filtered, 'Landline'):,}"),
        ("Mobiles", f"{count_present_values(filtered, 'Mobile'):,}"),
    ]
    for col, (label, value) in zip(metric_cols[:5], metric_values):
        with col:
            st.markdown(f'<div class="metric-tile"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)
    with metric_cols[5]:
        st.caption("Apply filters on the left, then review counts and generate exports or reports below.")
    st.markdown('</div>', unsafe_allow_html=True)

    top_left, top_mid, top_right = st.columns([1, 1, 1], gap="medium")
    with top_left:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        pie_chart_with_table(party_summary(filtered), "Party", "Individuals", "Party Mix", color_mode="party")
        st.markdown('</div>', unsafe_allow_html=True)
    with top_mid:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        pie_chart_with_table(age_band_summary(filtered), "Age Range", "Count", "Age Range", color_mode="age")
        st.markdown('</div>', unsafe_allow_html=True)
    with top_right:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        pie_chart_with_table(gender_summary(filtered), "Gender", "Count", "Gender", color_mode="gender")
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="table-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-header">Counts by Area</div>', unsafe_allow_html=True)
    area_choices = [c for c in ["County","Municipality","Ward","Precinct","School District","USC","STS","STH","MDJ"] if c in filtered.columns]
    area_col = area_choices[0] if area_choices else None
    if area_choices:
        selector_col, version_col = st.columns([1.25, 3.5])
        with selector_col:
            area_col = st.selectbox("Area", area_choices, label_visibility="collapsed")
        with version_col:
            st.caption("Click any column header below to sort within the currently filtered universe.")
        area_df = area_summary(filtered, area_col).copy()
        for ncol in ["Individuals", "Households"]:
            if ncol in area_df.columns:
                area_df[ncol] = pd.to_numeric(area_df[ncol], errors="coerce").fillna(0).astype(int)
        display_area_df = area_df.copy()
        for ncol in ["Individuals", "Households"]:
            if ncol in display_area_df.columns:
                display_area_df[ncol] = pd.to_numeric(display_area_df[ncol], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
        render_compact_table(display_area_df.head(250))
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="table-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-header">Exports and Reports</div>', unsafe_allow_html=True)
    st.markdown('<div class="report-note">Prepared files are saved into Files for Download so the user can copy them to Downloads at any time.</div>', unsafe_allow_html=True)

    csv_ready = excel_ready = household_ready = phone_ready = counts_ready = texting_ready = pdf_ready = update_ready = False
    finished_ready = bool(st.session_state.finished_downloads)
    finished_label = "Files for Download 🟢" if finished_ready else "Files for Download"
    export_tab, report_tab, finished_tab, data_tab = st.tabs(["Exports", "Reports", finished_label, "Data Update"])


    with export_tab:
        mail_mode_left, _mail_mode_spacer = st.columns([1.3, 3.7])
        with mail_mode_left:
            st.radio(
                "Mailing Mode",
                ["Not Householded", "Householded"],
                horizontal=True,
                key="mailing_mode",
            )
        current_mailing_mode = st.session_state.get("mailing_mode", "Not Householded")
        mail_file_title = "Mail File" if current_mailing_mode == "Not Householded" else "Householded Mail File"
        mail_file_sub = "Individual mailing file with one row per voter." if current_mailing_mode == "Not Householded" else "Householded USPS-style mailing file with smarter envelope naming."

        row1 = st.columns(3, gap="medium")
        export_actions = [
            ("Filtered CSV", "Full filtered voter file.", "csv_ready"),
            ("Excel Workbook", "Workbook with multiple tabs.", "excel_ready"),
            (mail_file_title, mail_file_sub, "household_ready"),
        ]
        for col, (title, sub, key_name) in zip(row1, export_actions):
            with col:
                st.markdown(f'<div class="action-card"><div class="action-title">{title}</div><div class="action-sub">{sub}</div></div>', unsafe_allow_html=True)
                if key_name == "csv_ready":
                    csv_ready = st.button(f"Prepare {title}", key=f"btn_{key_name}")
                elif key_name == "excel_ready":
                    excel_ready = st.button(f"Prepare {title}", key=f"btn_{key_name}")
                else:
                    household_ready = st.button(f"Prepare {title}", key=f"btn_{key_name}")

        row2 = st.columns(3, gap="medium")
        export_actions_2 = [
            ("Phone Bank File", "Names, address, landline, mobile, municipality, precinct.", "phone_ready"),
            ("Area Counts CSV", "Current counts by selected area view.", "counts_ready"),
            ("Texting CSV", "Voter ID, mobile, county, precinct for records with a mobile.", "texting_ready"),
        ]
        for col, (title, sub, key_name) in zip(row2, export_actions_2):
            with col:
                st.markdown(f'<div class="action-card"><div class="action-title">{title}</div><div class="action-sub">{sub}</div></div>', unsafe_allow_html=True)
                if key_name == "phone_ready":
                    phone_ready = st.button(f"Prepare {title}", key=f"btn_{key_name}")
                elif key_name == "counts_ready":
                    counts_ready = st.button(f"Prepare {title}", key=f"btn_{key_name}")
                else:
                    texting_ready = st.button(f"Prepare {title}", key=f"btn_{key_name}")

    with report_tab:
        r1 = st.columns(1, gap="medium")[0]
        with r1:
            st.markdown('<div class="action-card"><div class="action-title">Street List PDF</div><div class="action-sub">Walk-list style report saved to Files for Download.</div></div>', unsafe_allow_html=True)
            pdf_ready = st.button("Generate Street List PDF", key="btn_pdf_ready")
    with finished_tab:
        files_map = st.session_state.finished_downloads
        if files_map:
            st.markdown('<div class="ready-pill"><span class="ready-dot"></span>Files ready</div>', unsafe_allow_html=True)
            h1, h2, h3, h4, h5 = st.columns([2.7, .9, 1.2, .9, 1.1], gap="small")
            with h1:
                st.markdown('<div style="font-size:10px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.02em;text-align:left;">File</div>', unsafe_allow_html=True)
            with h2:
                st.markdown('<div style="font-size:10px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.02em;text-align:center;">Type</div>', unsafe_allow_html=True)
            with h3:
                st.markdown('<div style="font-size:10px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.02em;text-align:center;">Created</div>', unsafe_allow_html=True)
            with h4:
                st.markdown('<div style="font-size:10px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.02em;text-align:center;">Size</div>', unsafe_allow_html=True)
            with h5:
                st.markdown('<div style="font-size:10px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.02em;text-align:center;">Download</div>', unsafe_allow_html=True)
            st.markdown('<div style="border-bottom:1px solid #dedede;margin:.15rem 0 .35rem 0;"></div>', unsafe_allow_html=True)
            for label, payload in files_map.items():
                c1, c2, c3, c4, c5 = st.columns([2.7, .9, 1.2, .9, 1.1], gap="small")
                with c1:
                    st.markdown(f'<div style="font-size:11px;font-weight:600;color:#2f3134;padding:.08rem 0;">{escape(payload["filename"])}</div>', unsafe_allow_html=True)
                with c2:
                    st.markdown(f'<div style="font-size:10px;color:#555;text-align:center;padding:.08rem 0;">{escape(payload["kind"].title())}</div>', unsafe_allow_html=True)
                with c3:
                    st.markdown(f'<div style="font-size:10px;color:#555;text-align:center;padding:.08rem 0;">{escape(payload.get("created_at", ""))}</div>', unsafe_allow_html=True)
                with c4:
                    st.markdown(f'<div style="font-size:10px;color:#555;text-align:center;padding:.08rem 0;">{format_file_size(int(payload.get("size_bytes", 0)))}</div>', unsafe_allow_html=True)
                with c5:
                    st.download_button(
                        "Download",
                        data=payload["data"],
                        file_name=payload["filename"],
                        mime=payload["mime"],
                        key=f"download_{label}"
                    )
                st.markdown('<div style="border-bottom:1px solid #f0ebeb;margin:.15rem 0 .25rem 0;"></div>', unsafe_allow_html=True)
            a1, a2 = st.columns([.9, 4.1])
            with a1:
                if st.button("Clear List", key="clear_finished_folder"):
                    clear_finished_downloads_folder()
                    st.rerun()
        else:
            st.caption("No finished exports or reports yet.")

    with data_tab:

        d1, d2 = st.columns([1.45, 3.55])
        with d1:
            st.button("Refresh From Google Drive", disabled=True)
            update_ready = False
        with d2:
            st.caption("This web version loads the parquet file directly from Google Drive each time the app cache refreshes.")

    st.markdown('</div>', unsafe_allow_html=True)

if csv_ready:
    save_finished_download(
        "Filtered CSV",
        "filtered_voters.csv",
        prepare_csv_export(filtered).to_csv(index=False).encode("utf-8"),
        "text/csv",
        "export",
    )
    st.rerun()

if household_ready:
    householded_mode = st.session_state.get("mailing_mode", "Not Householded") == "Householded"
    file_label = "Householded Mail File" if householded_mode else "Mail File"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{'householded_mail_file' if householded_mode else 'mail_file'}_{timestamp}.csv"
    mail_df = build_mail_export(filtered, householded=householded_mode)
    save_finished_download(
        file_label,
        file_name,
        mail_df.to_csv(index=False).encode("utf-8"),
        "text/csv",
        "export",
    )
    sync_finished_downloads_from_disk()
    st.rerun()

if phone_ready:
    save_finished_download(
        "Phone Bank File",
        "phone_bank_file.csv",
        build_phone_bank_export(filtered).to_csv(index=False).encode("utf-8"),
        "text/csv",
        "export",
    )
    st.rerun()

if counts_ready and area_col:
    save_finished_download(
        "Area Counts CSV",
        f"counts_by_{area_col.lower().replace(' ', '_')}.csv",
        build_area_counts_export(filtered, area_col).to_csv(index=False).encode("utf-8"),
        "text/csv",
        "export",
    )
    st.rerun()

if texting_ready:
    save_finished_download(
        "Texting CSV",
        "texting_export.csv",
        build_texting_export(filtered).to_csv(index=False).encode("utf-8"),
        "text/csv",
        "export",
    )
    st.rerun()

if excel_ready and area_col:
    save_finished_download(
        "Excel Workbook",
        "candidate_connect_exports.xlsx",
        build_excel_export(filtered, area_col, st.session_state.filter_state, householded=(st.session_state.get("mailing_mode", "Not Householded") == "Householded")),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "export",
    )
    st.rerun()

if "Precinct" not in filtered.columns:
    st.warning("The PDF needs a Precinct column. I could not find one in this file.")
elif pdf_ready:
    with st.spinner("Building PDF report...", show_time=True):
        from pdf_utils import generate_walk_list_pdf
        pdf_bytes = generate_walk_list_pdf(
            filtered,
            report_title=largest_area_label(st.session_state.filter_state, filtered),
            report_description=selected_filter_summary(st.session_state.filter_state),
            selected_filters=st.session_state.filter_state.get("multi", {}),
            base_dir=BASE,
        )
    save_finished_download(
        "Street List PDF",
        "candidate_connect_report.pdf",
        pdf_bytes,
        "application/pdf",
        "report",
    )
    st.rerun()
