import streamlit as st
import pandas as pd
import gdown
from pathlib import Path
from io import BytesIO
import altair as alt
import base64

st.set_page_config(page_title="Candidate Connect", layout="wide")

DRIVE_FILE_ID = "1vQTn2pc1vuZiI8a0CyPvPA1k3jMOSNPt"
LOCAL_PARQUET = Path("/tmp/candidate_connect_data.parquet")
CC_LOGO = Path("candidate_connect_logo.png")
TSS_LOGO = Path("TSS_Logo_Transparent.png")

PARTY_COLOR_MAP = {"R": "#c62828", "D": "#1565c0", "O": "#2e7d32"}
AGE_COLOR_RANGE = ["#7a1523","#9f2032","#b8454f","#c96a6c","#d88f87","#e8b8aa","#f2dbcf","#f7ebe5","#fbf5f2"]
GENDER_COLOR_RANGE = ["#7a1523","#4b4f54","#b98088","#9b9da1","#d8b6bb"]

st.markdown("""
<style>
.block-container {padding-top: 1.35rem; padding-bottom: .75rem; max-width: 1600px;}
.top-shell, .section-card, .chart-card, .table-card, .export-card, .metric-card {
    border: 1px solid #ded7d7;
    border-radius: 14px;
    background: white;
    box-shadow: 0 1px 3px rgba(0,0,0,.04);
}
.top-shell {padding: 1.2rem 1rem 1rem 1rem; margin-top: .35rem; margin-bottom: .95rem; overflow: visible;}
.section-card, .chart-card, .table-card, .export-card {padding: .8rem .9rem; margin-bottom: .8rem;}
.metric-card {padding: .6rem .7rem; height: 94px; display:flex; flex-direction:column; justify-content:center;}
.metric-label {font-size: 11px; color: #666; margin-bottom: .12rem;}
.metric-value {font-size: 1.55rem; font-weight: 700; color: #24303f; line-height: 1.1;}
.small-header {font-size: 15px; font-weight: 800; color: #162235; margin-bottom: .45rem;}
.tiny-muted {font-size: 10px; color: #596579;}
.export-note {font-size:10px; color:#666; margin-top:.1rem;}
.stDownloadButton > button, .stButton > button {
    width:100%;
    border-radius:9px;
    min-height: 2.1rem;
    font-weight: 600;
}
div[data-testid="stDataFrame"] [role="row"] {min-height: 28px !important;}
section[data-testid="stSidebar"] .block-container {padding-top: 1rem;}
section[data-testid="stSidebar"] {border-right: 1px solid #e7e0e0;}
.cc-mini-table {width:100%; border-collapse:collapse; font-size:11px; margin-top:.35rem;}
.cc-mini-table th {text-align:center; padding:4px 6px; color:#364152; font-weight:800; border-bottom:1px solid #ece7e7;}
.cc-mini-table td {padding:4px 6px; border-bottom:1px solid #f0ebeb;}
.cc-mini-table td.label-cell {text-align:left;}
.cc-mini-table td.num-cell {text-align:center;}
.cc-mini-table tr.total-row td {font-weight:700; border-top:1px solid #dcd6d6;}
.cc-swatch {display:inline-block; width:9px; height:9px; border-radius:2px; vertical-align:middle; margin-right:8px; position:relative; top:-1px; border:1px solid rgba(0,0,0,.08);}
.brand-grid {display:grid; grid-template-columns: 200px 1fr 170px; gap:18px; align-items:center;}
.brand-left {display:flex; align-items:center; justify-content:flex-start; min-height:78px;}
.brand-center {display:flex; flex-direction:column; justify-content:center;}
.brand-right {display:flex; flex-direction:column; align-items:center; justify-content:center; min-height:78px;}
.brand-title {font-size: 24px; font-weight: 800; color:#153d73; line-height:1.05; margin-bottom:.12rem;}
.brand-sub {font-size: 11px; color:#334a6a; font-weight:700;}
.brand-status {font-size: 11px; color:#506078; margin-top:.28rem; font-weight:600;}
.powered-by {font-size:10px; color:#777; margin-bottom:.18rem; text-align:center; font-weight:700;}
.logo-cc {max-width:168px; height:auto; display:block;}
.logo-tss {max-width:102px; height:auto; display:block; margin:0 auto;}
.loading-banner {font-size:12px; font-weight:600; color:#245280;}
.section-divider {height:1px; background:linear-gradient(to right, rgba(0,0,0,0), #d7d1d1 12%, #d7d1d1 88%, rgba(0,0,0,0)); margin:.5rem 0 .8rem 0;}
.sidebar-note {font-size:10px; color:#687487; margin-top:-.25rem; margin-bottom:.4rem;}
@media (max-width: 1100px) {
  .brand-grid {grid-template-columns: 1fr; gap:10px;}
  .brand-left, .brand-right {justify-content:center;}
  .brand-center {text-align:center;}
}
</style>
""", unsafe_allow_html=True)


def img_to_data_uri(path: Path) -> str:
    if not path.exists():
        return ""
    encoded = base64.b64encode(path.read_bytes()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def smart_title(val):
    if pd.isna(val):
        return ""
    text = str(val).strip()
    if not text or text.lower() == "nan":
        return ""
    return " ".join(word.capitalize() for word in text.replace("_", " ").split())


def clean_text(val):
    if pd.isna(val):
        return ""
    text = str(val).strip()
    return "" if text.lower() in {"nan", "none"} else text


def value_present(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    return series.notna() & ~text.isin(["", "nan", "None"])


def fmt_pct(v: float) -> str:
    rounded = round(v, 1)
    if float(rounded).is_integer():
        return f"{int(rounded)}%"
    return f"{rounded:.1f}%"


def normalize_vote_history_value(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().replace({"NAN": "", "NONE": ""})


def normalize_boolish(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.upper()
    return s.replace({"TRUE":"Yes","FALSE":"No","Y":"Yes","N":"No","1":"Yes","0":"No","NAN":"","NONE":"","T":"Yes","F":"No"})


@st.cache_resource(show_spinner=True)
def load_data():
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"
    if not LOCAL_PARQUET.exists():
        gdown.download(url=url, output=str(LOCAL_PARQUET), quiet=False)
    df = pd.read_parquet(LOCAL_PARQUET)
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]

    status_col = "VoterStatus" if "VoterStatus" in df.columns else ("voterstatus" if "voterstatus" in df.columns else None)
    if status_col:
        df["_Status"] = df[status_col].astype(str).str.strip().str.upper()
        df = df[df["_Status"] == "A"].copy()
    else:
        df["_Status"] = "A"

    for col in ["County", "Municipality", "Precinct", "School District", "CalculatedParty", "USC", "STS", "STH", "HH-Party"]:
        if col in df.columns:
            df[col] = df[col].astype("object").map(smart_title)

    if "Age" in df.columns:
        df["_AgeNum"] = pd.to_numeric(df["Age"], errors="coerce")
    else:
        df["_AgeNum"] = pd.NA

    reg_col = "RegistrationDate" if "RegistrationDate" in df.columns else ("registrationdate" if "registrationdate" in df.columns else None)
    if reg_col:
        df["_RegistrationDate"] = pd.to_datetime(df[reg_col], errors="coerce")
    else:
        df["_RegistrationDate"] = pd.NaT

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

    for col in ["Email", "Landline", "Mobile", "MB_Perm"]:
        if col in df.columns:
            df[f"_Has{col}"] = value_present(df[col])
        else:
            df[f"_Has{col}"] = False

    df["_NewReg"] = normalize_boolish(df["TAG0003_New_Reg"]) if "TAG0003_New_Reg" in df.columns else ""
    df["_VoteHistory"] = normalize_vote_history_value(df["V4A"]) if "V4A" in df.columns else ""

    vm_cols = [c for c in df.columns if str(c).endswith("_VM")]
    for c in vm_cols:
        df[f"_{c}"] = normalize_vote_history_value(df[c])

    df["_MBPerm"] = normalize_boolish(df["MB_PERM"]) if "MB_PERM" in df.columns else ""
    df["_MIBProb"] = pd.to_numeric(df["MMB_AProp_Score"], errors="coerce") if "MMB_AProp_Score" in df.columns else pd.NA
    df["_MIBApplied"] = normalize_boolish(df["MIB_Applied"]) if "MIB_Applied" in df.columns else ""
    df["_MIBVoted"] = normalize_boolish(df["MIB_BALLOT"]) if "MIB_BALLOT" in df.columns else ""

    return df.reset_index(drop=True)


def household_key(frame: pd.DataFrame) -> pd.Series:
    if "HH_ID" in frame.columns:
        hh = frame["HH_ID"].astype(str).str.strip()
        if (hh != "").any():
            return hh.where(hh != "", frame.index.astype(str))
    parts = []
    for col in ["House Number", "Street Name", "Apartment Number"]:
        if col in frame.columns:
            parts.append(frame[col].astype(str).fillna(""))
    if not parts:
        return frame.index.astype(str)
    key = parts[0]
    for p in parts[1:]:
        key = key + "|" + p
    return key


def count_households(frame: pd.DataFrame) -> int:
    return household_key(frame).nunique() if len(frame) else 0


def build_area_summary(frame: pd.DataFrame, area_col: str) -> pd.DataFrame:
    temp = frame.copy()
    temp["_hh"] = household_key(temp)
    out = (
        temp.groupby(area_col, dropna=False)
        .agg(Individuals=(area_col, "size"), Households=("_hh", "nunique"))
        .reset_index()
        .sort_values("Individuals", ascending=False)
        .reset_index(drop=True)
    )
    out[area_col] = out[area_col].astype(object).where(out[area_col].notna(), "(Blank)")
    return out


def full_name_from_row(row: pd.Series) -> str:
    full = clean_text(row.get("FullName", ""))
    if full:
        return smart_title(full)
    parts = [
        clean_text(row.get("FirstName", "")),
        clean_text(row.get("MiddleName", "")),
        clean_text(row.get("LastName", "")),
        clean_text(row.get("NameSuffix", "")),
    ]
    joined = " ".join([p for p in parts if p]).strip()
    return smart_title(joined)


def build_address_line1(frame: pd.DataFrame) -> pd.Series:
    house = frame["House Number"].astype(str).fillna("") if "House Number" in frame.columns else pd.Series([""] * len(frame), index=frame.index)
    street = frame["Street Name"].astype(str).fillna("") if "Street Name" in frame.columns else pd.Series([""] * len(frame), index=frame.index)
    return (house + " " + street).str.replace(r"\s+", " ", regex=True).str.strip()


def build_address_line2(frame: pd.DataFrame) -> pd.Series:
    if "Apartment Number" not in frame.columns:
        return pd.Series([""] * len(frame), index=frame.index)
    apt = frame["Apartment Number"].astype(str).fillna("").replace({"nan": "", "None": ""})
    return apt.map(lambda x: f"Apt {x.strip()}" if str(x).strip() else "")


def first_existing_column(frame: pd.DataFrame, candidates):
    for col in candidates:
        if col in frame.columns:
            return col
    lowered = {str(c).strip().lower(): c for c in frame.columns}
    for col in candidates:
        key = col.strip().lower()
        if key in lowered:
            return lowered[key]
    return None


def resolve_city_state_zip(frame: pd.DataFrame):
    city_col = first_existing_column(frame, ["MailingCity", "Mailing City", "City", "MailCity"])
    state_col = first_existing_column(frame, ["MailingState", "Mailing State", "State", "MailState"])
    zip_col = first_existing_column(frame, ["MailingZip", "Mailing Zip", "ZIP", "Zip", "ZipCode", "ZIPCODE", "MailZip"])

    city = frame[city_col].map(smart_title) if city_col else pd.Series([""] * len(frame), index=frame.index)
    state = frame[state_col].astype(str).fillna("").str.strip() if state_col else pd.Series([""] * len(frame), index=frame.index)
    zipcode = frame[zip_col].astype(str).fillna("").str.strip() if zip_col else pd.Series([""] * len(frame), index=frame.index)

    if not city_col and "Municipality" in frame.columns:
        city = frame["Municipality"].map(smart_title)

    return city, state, zipcode


def resolve_voter_id_column(frame: pd.DataFrame):
    priority = ["PA ID Number", "State Voter ID", "Voter ID", "VoterID", "PA Voter ID"]
    for col in priority:
        if col in frame.columns:
            return col
    lowered = {str(c).strip().lower(): c for c in frame.columns}
    for key in ["pa id number", "state voter id", "voter id", "voterid", "pa voter id"]:
        if key in lowered:
            return lowered[key]
    return None


def household_display_name(group: pd.DataFrame) -> str:
    rows = group.copy()
    rows["_FullName"] = rows.apply(full_name_from_row, axis=1)
    names = [n for n in rows["_FullName"].tolist() if n]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    last_names = []
    for _, row in rows.iterrows():
        ln = clean_text(row.get("LastName", ""))
        if ln:
            last_names.append(smart_title(ln))
    unique_last = sorted(set([ln for ln in last_names if ln]))
    if len(unique_last) == 1:
        return f"{unique_last[0]} Household"
    unique_names = []
    for n in names:
        if n not in unique_names:
            unique_names.append(n)
    if len(unique_names) >= 2:
        return " & ".join(unique_names[:2])
    return unique_names[0]


def build_mail_export(frame: pd.DataFrame, householded: bool) -> pd.DataFrame:
    out = frame.copy()
    out["_FullName"] = out.apply(full_name_from_row, axis=1)
    out["_AddressLine1"] = build_address_line1(out)
    out["_AddressLine2"] = build_address_line2(out)
    out["_HouseholdKey"] = household_key(out)
    city, state, zipcode = resolve_city_state_zip(out)
    out["_City"] = city
    out["_State"] = state
    out["_Zip"] = zipcode
    for col in ["County", "Municipality", "Precinct"]:
        if col not in out.columns:
            out[col] = ""
    if not householded:
        export_df = pd.DataFrame({
            "MailName": out["_FullName"],
            "AddressLine1": out["_AddressLine1"],
            "AddressLine2": out["_AddressLine2"],
            "City": out["_City"],
            "State": out["_State"],
            "Zip": out["_Zip"],
            "Municipality": out["Municipality"].map(smart_title),
            "County": out["County"].map(smart_title),
            "Precinct": out["Precinct"].map(smart_title),
        })
        return export_df.reset_index(drop=True)
    rows = []
    for _, group in out.groupby("_HouseholdKey", dropna=False, sort=False):
        first = group.iloc[0]
        rows.append({
            "MailName": household_display_name(group),
            "AddressLine1": clean_text(first.get("_AddressLine1", "")),
            "AddressLine2": clean_text(first.get("_AddressLine2", "")),
            "City": clean_text(first.get("_City", "")),
            "State": clean_text(first.get("_State", "")),
            "Zip": clean_text(first.get("_Zip", "")),
            "Municipality": smart_title(first.get("Municipality", "")),
            "County": smart_title(first.get("County", "")),
            "Precinct": smart_title(first.get("Precinct", "")),
            "Individuals": len(group),
        })
    return pd.DataFrame(rows)


def build_texting_export(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "Mobile" not in out.columns:
        return pd.DataFrame(columns=["VoterID", "FirstName", "MiddleName", "LastName", "FullName", "Mobile", "County", "Precinct"])
    out["Mobile"] = out["Mobile"].astype(str).str.replace(r"\D", "", regex=True)
    out = out[out["Mobile"].str.strip() != ""].copy()
    voter_col = resolve_voter_id_column(out)
    out["VoterID"] = out[voter_col].astype(str).str.strip() if voter_col else ""
    out["FirstName"] = out["FirstName"].map(smart_title) if "FirstName" in out.columns else ""
    out["MiddleName"] = out["MiddleName"].map(smart_title) if "MiddleName" in out.columns else ""
    out["LastName"] = out["LastName"].map(smart_title) if "LastName" in out.columns else ""
    out["FullName"] = out.apply(full_name_from_row, axis=1)
    for col in ["County", "Precinct"]:
        if col not in out.columns:
            out[col] = ""
    export_df = out[["VoterID", "FirstName", "MiddleName", "LastName", "FullName", "Mobile", "County", "Precinct"]].copy()
    export_df["County"] = export_df["County"].map(smart_title)
    export_df["Precinct"] = export_df["Precinct"].map(smart_title)
    return export_df.reset_index(drop=True)


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Export") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def make_summary_table(df_chart: pd.DataFrame, label_col: str, value_col: str, colors):
    total = pd.to_numeric(df_chart[value_col], errors="coerce").fillna(0).sum()
    headers = "<tr><th></th><th>{}</th><th>{}</th><th>%</th></tr>".format(label_col, value_col)
    rows = []
    for i, (_, row) in enumerate(df_chart.iterrows()):
        val = float(pd.to_numeric(row[value_col], errors="coerce"))
        pct = 0 if total == 0 else (val / total) * 100
        color = colors[i] if i < len(colors) else "#999999"
        rows.append(
            f"<tr><td class='num-cell'><span class='cc-swatch' style='background:{color};'></span></td>"
            f"<td class='label-cell'>{row[label_col]}</td><td class='num-cell'>{val:,.0f}</td><td class='num-cell'>{fmt_pct(pct)}</td></tr>"
        )
    rows.append(f"<tr class='total-row'><td></td><td class='label-cell'>Total</td><td class='num-cell'>{total:,.0f}</td><td class='num-cell'>100%</td></tr>")
    return f"<table class='cc-mini-table'><thead>{headers}</thead><tbody>{''.join(rows)}</tbody></table>"


def pie_chart_with_table(df_chart: pd.DataFrame, label_col: str, value_col: str, title: str, color_mode: str):
    st.markdown(f'<div class="small-header">{title}</div>', unsafe_allow_html=True)
    if df_chart.empty:
        st.caption("No data")
        return
    chart_df = df_chart.copy()
    chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce").fillna(0)
    chart_df = chart_df.sort_values(value_col, ascending=False).reset_index(drop=True)
    total = chart_df[value_col].sum()
    chart_df["Percent"] = 0 if total == 0 else (chart_df[value_col] / total) * 100
    domain = chart_df[label_col].astype(str).tolist()
    if color_mode == "party":
        colors = [PARTY_COLOR_MAP.get(v, "#757575") for v in domain]
    elif color_mode == "age":
        colors = AGE_COLOR_RANGE[:len(domain)]
    else:
        colors = GENDER_COLOR_RANGE[:len(domain)]
    chart = alt.Chart(chart_df).mark_arc(innerRadius=18, outerRadius=60).encode(
        theta=alt.Theta(field=value_col, type="quantitative"),
        color=alt.Color(field=label_col, type="nominal", scale=alt.Scale(domain=domain, range=colors), legend=None),
        tooltip=[alt.Tooltip(f"{label_col}:N"), alt.Tooltip(f"{value_col}:Q", format=","), alt.Tooltip("Percent:Q", format=".1f")]
    ).properties(height=220)
    st.altair_chart(chart, use_container_width=True)
    st.markdown(make_summary_table(chart_df, label_col, value_col, colors), unsafe_allow_html=True)


def file_modified_text(path: Path) -> str:
    if not path.exists():
        return "Google Drive source"
    try:
        ts = pd.Timestamp(path.stat().st_mtime, unit="s")
        return ts.strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        return "Google Drive source"


def divider():
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)


cc_logo_uri = img_to_data_uri(CC_LOGO)
tss_logo_uri = img_to_data_uri(TSS_LOGO)

loading_box = st.empty()
loading_box.markdown('<div class="top-shell"><div class="small-header">Candidate Connect</div><div class="tiny-muted">Web dashboard for filters, charts, and exports</div></div>', unsafe_allow_html=True)
status_box = st.empty()
status_box.markdown('<div class="section-card"><div class="small-header loading-banner">Loading data from Google Drive...</div></div>', unsafe_allow_html=True)

try:
    df = load_data()
except Exception as e:
    status_box.empty()
    st.error(f"Error loading data: {e}")
    st.stop()

loading_box.empty()
status_box.empty()

header_html = f"""
<div class="top-shell">
  <div class="brand-grid">
    <div class="brand-left">{f'<img class="logo-cc" src="{cc_logo_uri}"/>' if cc_logo_uri else ''}</div>
    <div class="brand-center">
      <div class="brand-title">Candidate Connect</div>
      <div class="brand-sub">Voter Data &amp; Engagement Platform</div>
      <div class="brand-status">Data Source: Google Drive &nbsp;&nbsp;|&nbsp;&nbsp; Last Loaded File: {file_modified_text(LOCAL_PARQUET)} &nbsp;&nbsp;|&nbsp;&nbsp; Rows Available: {len(df):,}</div>
    </div>
    <div class="brand-right"><div class="powered-by">Powered By</div>{f'<img class="logo-tss" src="{tss_logo_uri}"/>' if tss_logo_uri else ''}</div>
  </div>
</div>
"""
st.markdown(header_html, unsafe_allow_html=True)

# Persistent filter state
if "active_filters" not in st.session_state:
    st.session_state.active_filters = {}

with st.sidebar:
    st.header("Filters")
    st.markdown('<div class="sidebar-note">Expanded filter set from the desktop version is being restored in stages.</div>', unsafe_allow_html=True)

    with st.form("filter_form", clear_on_submit=False):
        with st.expander("Geography", expanded=False):
            geo_cols = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in df.columns]
            geo_selections = {}
            for col in geo_cols:
                vals = df[col].dropna().astype(str).str.strip()
                vals = sorted([v for v in vals.unique().tolist() if v != ""])
                geo_selections[col] = st.multiselect(col, vals, default=st.session_state.active_filters.get(col, []))

        with st.expander("Voter Details", expanded=False):
            party_vals = sorted([v for v in df["Party"].dropna().astype(str).str.strip().unique().tolist() if v != ""]) if "Party" in df.columns else []
            gender_vals = sorted([v for v in df["_Gender"].dropna().astype(str).str.strip().unique().tolist() if v != ""])
            age_range_vals = sorted([v for v in df["_AgeRange"].dropna().astype(str).str.strip().unique().tolist() if v != ""])
            hh_party_vals = sorted([v for v in df["HH-Party"].dropna().astype(str).str.strip().unique().tolist() if v != ""]) if "HH-Party" in df.columns else []
            calc_party_vals = sorted([v for v in df["CalculatedParty"].dropna().astype(str).str.strip().unique().tolist() if v != ""]) if "CalculatedParty" in df.columns else []

            party_pick = st.multiselect("Party", party_vals, default=st.session_state.active_filters.get("party_pick", [])) if party_vals else []
            hh_party_pick = st.multiselect("Household Party", hh_party_vals, default=st.session_state.active_filters.get("hh_party_pick", [])) if hh_party_vals else []
            calc_party_pick = st.multiselect("Calculated Party", calc_party_vals, default=st.session_state.active_filters.get("calc_party_pick", [])) if calc_party_vals else []
            gender_pick = st.multiselect("Gender", gender_vals, default=st.session_state.active_filters.get("gender_pick", [])) if gender_vals else []
            age_range_pick = st.multiselect("Age Range", age_range_vals, default=st.session_state.active_filters.get("age_range_pick", [])) if age_range_vals else []

            age_slider = None
            if pd.to_numeric(df["_AgeNum"], errors="coerce").notna().any():
                age_min = int(pd.to_numeric(df["_AgeNum"], errors="coerce").min())
                age_max = int(pd.to_numeric(df["_AgeNum"], errors="coerce").max())
                age_slider = st.slider("Age", age_min, age_max, st.session_state.active_filters.get("age_slider", (age_min, age_max)))

        with st.expander("Vote History", expanded=False):
            new_reg_options = ["(Any)", "Newest 3 months in file", "Newest 6 months in file", "Newest 1 year in file"]
            vote_history_vals = sorted([v for v in df["_VoteHistory"].dropna().astype(str).str.strip().unique().tolist() if v != ""])

            vm_cols = [c for c in df.columns if str(c).endswith("_VM") and len(str(c)) >= 4]
            year_vals = sorted(list(set([f"20{str(c)[1:3]}" for c in vm_cols if str(c)[1:3].isdigit()])))
            type_map = {"G": "General Election", "P": "Primary Election"}
            type_options = []
            for c in vm_cols:
                t = str(c)[0].upper()
                label = type_map.get(t)
                if label and label not in type_options:
                    type_options.append(label)

            year_options = ["(Any)"] + year_vals
            type_options_full = ["(Any)"] + type_options
            method_options = ["(Any)", "AP", "MB", "P", "Did Not Vote"]

            vh_year = st.selectbox("Year", year_options, index=year_options.index(st.session_state.active_filters.get("vh_year", "(Any)")) if st.session_state.active_filters.get("vh_year", "(Any)") in year_options else 0)
            vh_type = st.selectbox("Election Type", type_options_full, index=type_options_full.index(st.session_state.active_filters.get("vh_type", "(Any)")) if st.session_state.active_filters.get("vh_type", "(Any)") in type_options_full else 0)
            vh_method = st.selectbox("Vote Method", method_options, index=method_options.index(st.session_state.active_filters.get("vh_method", "(Any)")) if st.session_state.active_filters.get("vh_method", "(Any)") in method_options else 0)

            new_reg_pick = st.selectbox("Newly Registered", new_reg_options, index=new_reg_options.index(st.session_state.active_filters.get("new_reg_pick", "(Any)")) if st.session_state.active_filters.get("new_reg_pick", "(Any)") in new_reg_options else 0)
            vote_history_pick = st.multiselect("Vote History", vote_history_vals, default=st.session_state.active_filters.get("vote_history_pick", [])) if vote_history_vals else []

        with st.expander("Mail In Ballots", expanded=False):
            mib_perm_vals = sorted([v for v in df["_MBPerm"].dropna().astype(str).str.strip().unique().tolist() if v != ""])
            mib_applied_vals = sorted([v for v in df["_MIBApplied"].dropna().astype(str).str.strip().unique().tolist() if v != ""])
            mib_voted_vals = sorted([v for v in df["_MIBVoted"].dropna().astype(str).str.strip().unique().tolist() if v != ""])

            mib_perm_pick = st.multiselect("MIB Perm", mib_perm_vals, default=st.session_state.active_filters.get("mib_perm_pick", [])) if mib_perm_vals else []
            mib_applied_pick = st.multiselect("MIB Applied", mib_applied_vals, default=st.session_state.active_filters.get("mib_applied_pick", [])) if mib_applied_vals else []
            mib_voted_pick = st.multiselect("MIB Voted", mib_voted_vals, default=st.session_state.active_filters.get("mib_voted_pick", [])) if mib_voted_vals else []

            mib_prob_slider = None
            if pd.to_numeric(df["_MIBProb"], errors="coerce").notna().any():
                mib_min = float(pd.to_numeric(df["_MIBProb"], errors="coerce").min())
                mib_max = float(pd.to_numeric(df["_MIBProb"], errors="coerce").max())
                mib_prob_slider = st.slider("MIB Probability Score", mib_min, mib_max, st.session_state.active_filters.get("mib_prob_slider", (mib_min, mib_max)))

        with st.expander("Tags", expanded=False):
            tag_map = {
                "Pro 2A": "TAG0001_Pro2A",
                "FOAC Target": "TAG00011_Pro2A_FOAC_TARG",
                "MB Target": "TAG0002_MB_Target",
                "Pro Life": "TAG0004_ProLife",
                "Pro Labor": "TAG0005_ProLabor",
                "Rep Donor": "TAG0006_RepDonor",
                "Dem Donor": "TAG0007_DemDonor",
                "Trump Donor": "TAG0008_TrumpDonor",
                "PA Donor": "TAG00090_PADonor",
                "Federal Donor": "TAG00100_FedDonor",
                "Any Donor": "TAG00110_AllDonor",
                "Teacher": "TAG0014_Teacher",
                "Retired Teacher": "TAG0015_RetiredTeacher"
            }
            tag_options = ["(None)"] + list(tag_map.keys())
            tag_choice = st.selectbox("Select Tag", tag_options, index=tag_options.index(st.session_state.active_filters.get("tag_choice", "(None)")) if st.session_state.active_filters.get("tag_choice", "(None)") in tag_options else 0)

        with st.expander("Contact Filters", expanded=False):
            email_opts = ["All", "Has Email", "No Email"]
            landline_opts = ["All", "Has Landline", "No Landline"]
            mobile_opts = ["All", "Has Mobile", "No Mobile"]
            has_email = st.selectbox("Email", email_opts, index=email_opts.index(st.session_state.active_filters.get("has_email", "All")) if st.session_state.active_filters.get("has_email", "All") in email_opts else 0)
            has_landline = st.selectbox("Landline", landline_opts, index=landline_opts.index(st.session_state.active_filters.get("has_landline", "All")) if st.session_state.active_filters.get("has_landline", "All") in landline_opts else 0)
            has_mobile = st.selectbox("Mobile", mobile_opts, index=mobile_opts.index(st.session_state.active_filters.get("has_mobile", "All")) if st.session_state.active_filters.get("has_mobile", "All") in mobile_opts else 0)

        cols = st.columns(2)
        apply_filters = cols[0].form_submit_button("Apply Filters", use_container_width=True)
        clear_filters = cols[1].form_submit_button("Clear Filters", use_container_width=True)

    if clear_filters:
        st.session_state.active_filters = {}
        st.rerun()

    if apply_filters:
        st.session_state.active_filters = {
            **geo_selections,
            "party_pick": party_pick,
            "hh_party_pick": hh_party_pick,
            "calc_party_pick": calc_party_pick,
            "gender_pick": gender_pick,
            "age_range_pick": age_range_pick,
            "age_slider": age_slider,
            "vh_year": vh_year,
            "vh_type": vh_type,
            "vh_method": vh_method,
            "new_reg_pick": new_reg_pick,
            "vote_history_pick": vote_history_pick,
            "mib_perm_pick": mib_perm_pick,
            "mib_applied_pick": mib_applied_pick,
            "mib_voted_pick": mib_voted_pick,
            "mib_prob_slider": mib_prob_slider,
            "tag_choice": tag_choice,
            "has_email": has_email,
            "has_landline": has_landline,
            "has_mobile": has_mobile,
        }

active = st.session_state.active_filters
geo_selections = {k: active.get(k, []) for k in ["County","Municipality","Precinct","USC","STS","STH","School District"]}
party_pick = active.get("party_pick", [])
hh_party_pick = active.get("hh_party_pick", [])
calc_party_pick = active.get("calc_party_pick", [])
gender_pick = active.get("gender_pick", [])
age_range_pick = active.get("age_range_pick", [])
age_slider = active.get("age_slider", None)
vh_year = active.get("vh_year", "(Any)")
vh_type = active.get("vh_type", "(Any)")
vh_method = active.get("vh_method", "(Any)")
new_reg_pick = active.get("new_reg_pick", "(Any)")
vote_history_pick = active.get("vote_history_pick", [])
mib_perm_pick = active.get("mib_perm_pick", [])
mib_applied_pick = active.get("mib_applied_pick", [])
mib_voted_pick = active.get("mib_voted_pick", [])
mib_prob_slider = active.get("mib_prob_slider", None)
tag_choice = active.get("tag_choice", "(None)")
has_email = active.get("has_email", "All")
has_landline = active.get("has_landline", "All")
has_mobile = active.get("has_mobile", "All")

filtered = df.copy()

for col, picked in geo_selections.items():
    if picked and col in filtered.columns:
        filtered = filtered[filtered[col].astype(str).isin(picked)]

if party_pick:
    filtered = filtered[filtered["Party"].astype(str).isin(party_pick)]
if hh_party_pick and "HH-Party" in filtered.columns:
    filtered = filtered[filtered["HH-Party"].astype(str).isin(hh_party_pick)]
if calc_party_pick and "CalculatedParty" in filtered.columns:
    filtered = filtered[filtered["CalculatedParty"].astype(str).isin(calc_party_pick)]
if gender_pick:
    filtered = filtered[filtered["_Gender"].astype(str).isin(gender_pick)]
if age_range_pick:
    filtered = filtered[filtered["_AgeRange"].astype(str).isin(age_range_pick)]
if age_slider is not None:
    filtered = filtered[(filtered["_AgeNum"] >= age_slider[0]) & (filtered["_AgeNum"] <= age_slider[1])]

reg_debug = pd.to_datetime(filtered["_RegistrationDate"], errors="coerce")
latest_reg = reg_debug.max()
if pd.notna(latest_reg):
    st.caption(f"Newest registration date in file: {latest_reg.strftime('%m/%d/%Y')}")


if new_reg_pick != "(Any)":
    reg_dates = pd.to_datetime(filtered["_RegistrationDate"], errors="coerce")
    valid_mask = reg_dates.notna()
    latest_reg = reg_dates.max()

    if pd.notna(latest_reg):
        if new_reg_pick == "Newest 3 months in file":
            cutoff = latest_reg - pd.DateOffset(months=3)
        elif new_reg_pick == "Newest 6 months in file":
            cutoff = latest_reg - pd.DateOffset(months=6)
        else:
            cutoff = latest_reg - pd.DateOffset(years=1)

        filtered = filtered[valid_mask & (reg_dates >= cutoff)]
    else:
        filtered = filtered.iloc[0:0]
if vote_history_pick:
    filtered = filtered[filtered["_VoteHistory"].astype(str).isin(vote_history_pick)]

if vh_year != "(Any)" or vh_type != "(Any)" or vh_method != "(Any)":
    vm_cols = [c for c in filtered.columns if str(c).endswith("_VM") and len(str(c)) >= 4]
    type_reverse_map = {"General Election": "G", "Primary Election": "P"}
    selected_cols = []
    for c in vm_cols:
        c_str = str(c)
        col_type = c_str[0].upper()
        col_year = f"20{c_str[1:3]}" if c_str[1:3].isdigit() else ""
        type_match = (vh_type == "(Any)" or col_type == type_reverse_map.get(vh_type, ""))
        year_match = (vh_year == "(Any)" or col_year == vh_year)
        if type_match and year_match:
            selected_cols.append(c)

    if selected_cols:
        if vh_method == "(Any)":
            row_mask = filtered[selected_cols].astype(str).apply(
                lambda row: any(str(v).strip().upper() not in {"", "NAN", "NONE"} for v in row), axis=1
            )
        elif vh_method == "Did Not Vote":
            row_mask = filtered[selected_cols].astype(str).apply(
                lambda row: all(str(v).strip().upper() in {"", "NAN", "NONE"} for v in row), axis=1
            )
        else:
            row_mask = filtered[selected_cols].astype(str).apply(
                lambda row: any(str(v).strip().upper() == vh_method for v in row), axis=1
            )
        filtered = filtered[row_mask]
    else:
        filtered = filtered.iloc[0:0]

if mib_perm_pick:
    filtered = filtered[filtered["_MBPerm"].astype(str).isin(mib_perm_pick)]
if mib_applied_pick:
    filtered = filtered[filtered["_MIBApplied"].astype(str).isin(mib_applied_pick)]
if mib_voted_pick:
    filtered = filtered[filtered["_MIBVoted"].astype(str).isin(mib_voted_pick)]
if mib_prob_slider is not None:
    filtered = filtered[(filtered["_MIBProb"] >= mib_prob_slider[0]) & (filtered["_MIBProb"] <= mib_prob_slider[1])]

if tag_choice != "(None)":
    tag_map = {
        "Pro 2A": "TAG0001_Pro2A",
        "FOAC Target": "TAG00011_Pro2A_FOAC_TARG",
        "MB Target": "TAG0002_MB_Target",
        "Pro Life": "TAG0004_ProLife",
        "Pro Labor": "TAG0005_ProLabor",
        "Rep Donor": "TAG0006_RepDonor",
        "Dem Donor": "TAG0007_DemDonor",
        "Trump Donor": "TAG0008_TrumpDonor",
        "PA Donor": "TAG00090_PADonor",
        "Federal Donor": "TAG00100_FedDonor",
        "Any Donor": "TAG00110_AllDonor",
        "Teacher": "TAG0014_Teacher",
        "Retired Teacher": "TAG0015_RetiredTeacher"
    }
    tag_col = tag_map.get(tag_choice)
    if tag_col in filtered.columns:
        tag_series = filtered[tag_col].astype(str).str.strip().str.upper()
        filtered = filtered[tag_series.isin(["Y", "YES", "TRUE", "1"])]

if has_email == "Has Email":
    filtered = filtered[filtered["_HasEmail"]]
elif has_email == "No Email":
    filtered = filtered[~filtered["_HasEmail"]]

if has_landline == "Has Landline":
    filtered = filtered[filtered["_HasLandline"]]
elif has_landline == "No Landline":
    filtered = filtered[~filtered["_HasLandline"]]

if has_mobile == "Has Mobile":
    filtered = filtered[filtered["_HasMobile"]]
elif has_mobile == "No Mobile":
    filtered = filtered[~filtered["_HasMobile"]]

filtered = filtered.reset_index(drop=True)

metric_cols = st.columns(7, gap="small")
metric_values = [
    ("Voters", f"{len(filtered):,}"),
    ("Households", f"{count_households(filtered):,}"),
    ("Emails", f"{int(filtered['_HasEmail'].sum()):,}"),
    ("Landlines", f"{int(filtered['_HasLandline'].sum()):,}"),
    ("Mobiles", f"{int(filtered['_HasMobile'].sum()):,}"),
    ("Unique Counties", f"{filtered['County'].nunique() if 'County' in filtered.columns else 0:,}"),
    ("Unique Precincts", f"{filtered['Precinct'].nunique() if 'Precinct' in filtered.columns else 0:,}"),
]
for col, (label, value) in zip(metric_cols, metric_values):
    with col:
        st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)

divider()

chart_cols = st.columns(3, gap="medium")
party_df = filtered["Party"].value_counts().rename_axis("Party").reset_index(name="Count") if "Party" in filtered.columns else pd.DataFrame(columns=["Party", "Count"])
gender_df = filtered["_Gender"].value_counts().rename_axis("Gender").reset_index(name="Count")
age_series = filtered["_AgeRange"].replace("", pd.NA).dropna()
age_df = age_series.value_counts().rename_axis("Age Range").reset_index(name="Count") if len(age_series) > 0 else pd.DataFrame(columns=["Age Range", "Count"])

with chart_cols[0]:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    pie_chart_with_table(party_df, "Party", "Count", "Party Breakdown", "party")
    st.markdown('</div>', unsafe_allow_html=True)
with chart_cols[1]:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    pie_chart_with_table(gender_df, "Gender", "Count", "Gender Breakdown", "gender")
    st.markdown('</div>', unsafe_allow_html=True)
with chart_cols[2]:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    pie_chart_with_table(age_df, "Age Range", "Count", "Age Range Breakdown", "age")
    st.markdown('</div>', unsafe_allow_html=True)

divider()

st.markdown('<div class="table-card">', unsafe_allow_html=True)
st.markdown('<div class="small-header">Counts by Area</div>', unsafe_allow_html=True)
area_choices = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in filtered.columns]
if area_choices:
    selected_area = st.selectbox("Area", area_choices, label_visibility="collapsed")
    area_df = build_area_summary(filtered, selected_area).copy()
    area_df["Individuals"] = pd.to_numeric(area_df["Individuals"], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
    area_df["Households"] = pd.to_numeric(area_df["Households"], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
    rows_html = "".join(
        f"<tr><td class='label-cell'>{row[selected_area]}</td><td class='num-cell'>{row['Individuals']}</td><td class='num-cell'>{row['Households']}</td></tr>"
        for _, row in area_df.iterrows()
    )
    table_html = f"<table class='cc-mini-table' style='font-size:12px;'><thead><tr><th style='text-align:left'>{selected_area}</th><th>Individuals</th><th>Households</th></tr></thead><tbody>{rows_html}</tbody></table>"
    st.markdown(table_html, unsafe_allow_html=True)
else:
    st.caption("No area columns found")
st.markdown('</div>', unsafe_allow_html=True)

divider()

st.markdown('<div class="table-card">', unsafe_allow_html=True)
st.markdown('<div class="small-header">Preview</div>', unsafe_allow_html=True)
st.dataframe(filtered.head(100), use_container_width=True, hide_index=True)
st.markdown('</div>', unsafe_allow_html=True)

divider()

st.markdown('<div class="export-card">', unsafe_allow_html=True)
st.markdown('<div class="small-header">Exports</div>', unsafe_allow_html=True)
st.markdown('<div class="export-note">Web version downloads files directly through your browser.</div>', unsafe_allow_html=True)

ex1, ex2, ex3 = st.columns([1.4, 1, 1])
with ex1:
    household_mode = st.radio("Mailing Mode", ["Not Householded", "Householded"], horizontal=True)

mail_df = build_mail_export(filtered, householded=(household_mode == "Householded"))
texting_df = build_texting_export(filtered)
mail_csv = mail_df.to_csv(index=False).encode("utf-8")
mail_xlsx = dataframe_to_excel_bytes(mail_df, "Mail File")
texting_csv = texting_df.to_csv(index=False).encode("utf-8")

with ex2:
    st.download_button("Download Mail CSV", data=mail_csv, file_name="candidate_connect_mail_file.csv", mime="text/csv", use_container_width=True)
    st.download_button("Download Mail Excel", data=mail_xlsx, file_name="candidate_connect_mail_file.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
with ex3:
    st.download_button("Download Texting CSV", data=texting_csv, file_name="candidate_connect_texting_file.csv", mime="text/csv", use_container_width=True)
    st.caption(f"Mail rows: {len(mail_df):,} | Text rows: {len(texting_df):,}")

st.markdown('</div>', unsafe_allow_html=True)
