import json
from pathlib import Path
import base64
import re
import io
from datetime import datetime


import altair as alt
import duckdb
import pandas as pd
import requests
import streamlit as st

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


st.set_page_config(page_title="Candidate Connect", layout="wide")

# R2 public-read setup
R2_BASE = "https://pub-a9e33b718082407cbd85e7b86b0fcb5c.r2.dev"
R2_BUCKET = "candidate-connect-data"

LOCAL_ROOT = Path("/tmp/candidate_connect_r2")
LOCAL_MANIFEST = LOCAL_ROOT / "dataset_manifest.json"

CC_LOGO = Path("candidate_connect_logo.png")
TSS_LOGO = Path("TSS_Logo_Transparent.png")

PARTY_COLOR_MAP = {"R": "#c62828", "D": "#1565c0", "O": "#2e7d32"}
AGE_COLOR_RANGE = ["#7a1523","#9f2032","#b8454f","#c96a6c","#d88f87","#e8b8aa","#f2dbcf","#f7ebe5","#fbf5f2"]
GENDER_COLOR_RANGE = ["#7a1523","#4b4f54","#b98088","#9b9da1","#d8b6bb"]

st.markdown("""
<style>
.block-container {padding-top: 1.35rem; padding-bottom: .75rem; max-width: 1600px;}
.top-shell, .section-card, .chart-card, .table-card, .metric-card {
    border: 1px solid #ded7d7;
    border-radius: 14px;
    background: white;
    box-shadow: 0 1px 3px rgba(0,0,0,.04);
}
.top-shell {padding: 1.2rem 1rem 1rem 1rem; margin-top: .35rem; margin-bottom: .95rem; overflow: visible;}
.section-card, .chart-card, .table-card {padding: .8rem .9rem; margin-bottom: .8rem;}
.metric-card {padding: .6rem .7rem; height: 94px; display:flex; flex-direction:column; justify-content:center;}
.metric-label {font-size: 11px; color: #666; margin-bottom: .12rem;}
.metric-value {font-size: 1.55rem; font-weight: 700; color: #24303f; line-height: 1.1;}
.small-header {font-size: 16px; font-weight: 900; color: #142033; margin-bottom: .45rem;}
.tiny-muted {font-size: 10px; color: #596579;}
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
.section-divider {height:1px; background:linear-gradient(to right, rgba(0,0,0,0), #d7d1d1 12%, #d7d1d1 88%, rgba(0,0,0,0)); margin:.5rem 0 .8rem 0;}
.sidebar-note {font-size:10px; color:#687487; margin-top:-.25rem; margin-bottom:.4rem;}
.stButton > button {width:100%; border-radius:9px; min-height: 2.1rem; font-weight: 600;}
.cc-mini-table {width:100%; border-collapse:collapse; font-size:11px; margin-top:.35rem;}
.cc-mini-table th {text-align:center; padding:4px 6px; color:#364152; font-weight:800; border-bottom:1px solid #ece7e7;}
.cc-mini-table td {padding:4px 6px; border-bottom:1px solid #f0ebeb;}
.cc-mini-table td.label-cell {text-align:left;}
.cc-mini-table td.num-cell {text-align:center;}
.cc-mini-table tr.total-row td {font-weight:700; border-top:1px solid #dcd6d6;}
.cc-swatch {display:inline-block; width:9px; height:9px; border-radius:2px; vertical-align:middle; margin-right:8px; position:relative; top:-1px; border:1px solid rgba(0,0,0,.08);}
.empty-shell {padding: 1.2rem 1rem; text-align:center; color:#556273;}
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

def file_modified_text(path: Path) -> str:
    if not path.exists():
        return "R2 public source"
    try:
        ts = pd.Timestamp(path.stat().st_mtime, unit="s")
        return ts.strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        return "R2 public source"

def divider():
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

def sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"

@st.cache_resource(show_spinner=False)
def get_conn():
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    return con

def first_existing(columns, candidates):
    lower_map = {str(c).strip().lower(): c for c in columns}
    for col in candidates:
        if col in columns:
            return col
        hit = lower_map.get(str(col).strip().lower())
        if hit is not None:
            return hit
    return None

def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def r2_public_url(key: str) -> str:
    return f"{R2_BASE}/{key}"

def download_public_object(key: str, local_path: Path):
    if local_path.exists():
        return
    ensure_parent(local_path)
    url = r2_public_url(key)
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

@st.cache_data(show_spinner=True)
def load_manifest():
    LOCAL_ROOT.mkdir(parents=True, exist_ok=True)
    download_public_object("dataset_manifest.json", LOCAL_MANIFEST)
    return json.loads(LOCAL_MANIFEST.read_text(encoding="utf-8"))

@st.cache_data(show_spinner=True)
def ensure_index_shards():
    manifest = load_manifest()
    local_paths = []
    for shard in manifest["index"]["shards"]:
        key = shard["key"]
        local_path = LOCAL_ROOT / key
        download_public_object(key, local_path)
        local_paths.append(str(local_path))
    return local_paths, manifest

@st.cache_data(show_spinner=False)
def get_schema(local_paths):
    con = get_conn()
    paths_sql = "[" + ", ".join(sql_string_literal(p) for p in local_paths) + "]"
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet({paths_sql})").df()
    return df["column_name"].tolist()

def build_view_sql(columns, local_paths):
    q = quote_ident
    status_col = first_existing(columns, ["VoterStatus", "voterstatus"])
    gender_col = first_existing(columns, ["Gender", "Sex"])
    age_range_col = first_existing(columns, ["Age_Range", "Age Range", "AGERANGE"])
    reg_col = first_existing(columns, ["RegistrationDate", "registrationdate"])
    party_col = first_existing(columns, ["Party"])
    hh_col = first_existing(columns, ["HH_ID"])
    email_col = first_existing(columns, ["Email"])
    landline_col = first_existing(columns, ["Landline"])
    mobile_col = first_existing(columns, ["Mobile"])
    vote_hist_col = first_existing(columns, ["V4A"])
    mib_applied_col = first_existing(columns, ["MIB_Applied"])
    mib_ballot_col = first_existing(columns, ["MIB_BALLOT"])
    mb_score_col = first_existing(columns, ["MMB_AProp_Score"])
    mb_perm_col = first_existing(columns, ["MB_PERM", "MB_Perm", "MB_Pern"])
    age_col = first_existing(columns, ["Age"])
    house_col = first_existing(columns, ["House Number"])
    street_col = first_existing(columns, ["Street Name"])
    apt_col = first_existing(columns, ["Apartment Number"])

    exprs = ["*"]

    if status_col:
        exprs.append(f"upper(trim(coalesce(cast({q(status_col)} as varchar), ''))) as _Status")
    else:
        exprs.append("'A' as _Status")

    if party_col:
        exprs.append(
            f"""case
                when upper(trim(coalesce(cast({q(party_col)} as varchar), ''))) in ('', 'NONE', 'NAN', 'U') then 'O'
                else upper(trim(cast({q(party_col)} as varchar)))
            end as _PartyNorm"""
        )
    else:
        exprs.append("'O' as _PartyNorm")

    if gender_col:
        exprs.append(
            f"""case
                when upper(trim(coalesce(cast({q(gender_col)} as varchar), ''))) in ('', 'NONE', 'NAN') then 'U'
                else upper(trim(cast({q(gender_col)} as varchar)))
            end as _Gender"""
        )
    else:
        exprs.append("'U' as _Gender")

    if age_col:
        exprs.append(f"try_cast({q(age_col)} as double) as _AgeNum")
    else:
        exprs.append("NULL::DOUBLE as _AgeNum")

    if age_range_col:
        exprs.append(f"nullif(trim(coalesce(cast({q(age_range_col)} as varchar), '')), '') as _AgeRange")
    else:
        exprs.append("NULL::VARCHAR as _AgeRange")

    if reg_col:
        exprs.append(f"try_cast({q(reg_col)} as timestamp) as _RegistrationDate")
    else:
        exprs.append("NULL::TIMESTAMP as _RegistrationDate")

    for alias, src in [("_HasEmail", email_col), ("_HasLandline", landline_col), ("_HasMobile", mobile_col)]:
        if src:
            exprs.append(
                f"""case
                    when trim(coalesce(cast({q(src)} as varchar), '')) in ('', 'None', 'NONE', 'nan', 'NAN') then false
                    else true
                end as {alias}"""
            )
        else:
            exprs.append(f"false as {alias}")

    if vote_hist_col:
        exprs.append(f"upper(trim(coalesce(cast({q(vote_hist_col)} as varchar), ''))) as _VoteHistory")
    else:
        exprs.append("'' as _VoteHistory")

    if mib_applied_col:
        exprs.append(f"upper(trim(coalesce(cast({q(mib_applied_col)} as varchar), ''))) as _MIBApplied")
    else:
        exprs.append("'' as _MIBApplied")

    if mib_ballot_col:
        exprs.append(f"upper(trim(coalesce(cast({q(mib_ballot_col)} as varchar), ''))) as _MIBBallot")
    else:
        exprs.append("'' as _MIBBallot")

    if mb_score_col:
        exprs.append(f"try_cast({q(mb_score_col)} as double) as _MBScore")
    else:
        exprs.append("NULL::DOUBLE as _MBScore")

    if mb_perm_col:
        exprs.append(f"""case
            when upper(trim(coalesce(cast({q(mb_perm_col)} as varchar), ''))) in ('TRUE', 'T', 'YES', 'Y', '1') then 'Y'
            when upper(trim(coalesce(cast({q(mb_perm_col)} as varchar), ''))) in ('FALSE', 'F', 'NO', 'N', '0') then 'N'
            else ''
        end as _MBPerm""")
    else:
        exprs.append("'' as _MBPerm")

    if hh_col:
        exprs.append(f"nullif(trim(coalesce(cast({q(hh_col)} as varchar), '')), '') as _HouseholdKey")
    else:
        parts = []
        if house_col:
            parts.append(f"coalesce(cast({q(house_col)} as varchar), '')")
        if street_col:
            parts.append(f"coalesce(cast({q(street_col)} as varchar), '')")
        if apt_col:
            parts.append(f"coalesce(cast({q(apt_col)} as varchar), '')")
        if parts:
            exprs.append("concat_ws('|', " + ", ".join(parts) + ") as _HouseholdKey")
        else:
            exprs.append("NULL::VARCHAR as _HouseholdKey")

    paths_sql = "[" + ", ".join(sql_string_literal(p) for p in local_paths) + "]"
    return "CREATE OR REPLACE VIEW voters AS SELECT\n    " + ",\n    ".join(exprs) + f"\nFROM read_parquet({paths_sql})"

def prepare_db(local_paths):
    con = get_conn()
    cols = get_schema(local_paths)
    con.execute(build_view_sql(cols, local_paths))
    return cols

def sql_literal_list(values):
    return ", ".join(["?"] * len(values))

def current_filter_clause(active, columns):
    where = ["_Status = 'A'"]
    params = []
    geo_cols = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in columns]
    for col in geo_cols:
        picked = active.get(col, [])
        if picked:
            where.append(f"{quote_ident(col)} IN ({sql_literal_list(picked)})")
            params.extend(picked)
    if active.get("party_pick"):
        picked = active["party_pick"]
        where.append(f"_PartyNorm IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("hh_party_pick") and "HH-Party" in columns:
        picked = active["hh_party_pick"]
        where.append(f'{quote_ident("HH-Party")} IN ({sql_literal_list(picked)})')
        params.extend(picked)
    if active.get("calc_party_pick") and "CalculatedParty" in columns:
        picked = active["calc_party_pick"]
        where.append(f'{quote_ident("CalculatedParty")} IN ({sql_literal_list(picked)})')
        params.extend(picked)
    if active.get("gender_pick"):
        picked = active["gender_pick"]
        where.append(f"_Gender IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("age_range_pick"):
        picked = active["age_range_pick"]
        where.append(f"_AgeRange IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("age_slider") is not None:
        where.append("_AgeNum >= ? AND _AgeNum <= ?")
        params.extend([active["age_slider"][0], active["age_slider"][1]])
    if active.get("vote_history_pick"):
        picked = active["vote_history_pick"]
        where.append(f"_VoteHistory IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("mib_applied_pick"):
        picked = active["mib_applied_pick"]
        where.append(f"_MIBApplied IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("mib_ballot_pick"):
        picked = active["mib_ballot_pick"]
        where.append(f"_MIBBallot IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("mb_perm_pick"):
        picked = active["mb_perm_pick"]
        where.append(f"_MBPerm IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("mb_score_slider") is not None:
        where.append("_MBScore >= ? AND _MBScore <= ?")
        params.extend([active["mb_score_slider"][0], active["mb_score_slider"][1]])
    if active.get("new_reg_months", 0) and active.get("new_reg_months", 0) > 0:
        where.append("_RegistrationDate >= (CURRENT_DATE - (? * INTERVAL '1 month'))")
        params.append(int(active["new_reg_months"]))
    if active.get("has_email") == "Has Email":
        where.append("_HasEmail = true")
    elif active.get("has_email") == "No Email":
        where.append("_HasEmail = false")
    if active.get("has_landline") == "Has Landline":
        where.append("_HasLandline = true")
    elif active.get("has_landline") == "No Landline":
        where.append("_HasLandline = false")
    if active.get("has_mobile") == "Has Mobile":
        where.append("_HasMobile = true")
    elif active.get("has_mobile") == "No Mobile":
        where.append("_HasMobile = false")
    return " WHERE " + " AND ".join(where), params

def get_distinct_options(column: str, label_expr: str | None = None):
    con = get_conn()
    expr = label_expr or quote_ident(column)
    df = con.execute(
        f"""
        SELECT {expr} AS value
        FROM voters
        WHERE _Status = 'A' AND nullif(trim(cast({quote_ident(column)} as varchar)), '') IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    return [str(v) for v in df["value"].tolist() if str(v).strip() != ""]

def get_basic_options(columns):
    options = {}
    geo_cols = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in columns]
    for col in geo_cols:
        options[col] = get_distinct_options(col)
    options["party_vals"] = get_distinct_options("_PartyNorm", "_PartyNorm") if "Party" in columns else []
    options["gender_vals"] = get_distinct_options("_Gender", "_Gender")
    options["age_range_vals"] = get_distinct_options("_AgeRange", "_AgeRange")
    options["hh_party_vals"] = get_distinct_options("HH-Party") if "HH-Party" in columns else []
    options["calc_party_vals"] = get_distinct_options("CalculatedParty") if "CalculatedParty" in columns else []
    options["vote_history_vals"] = ordered_vote_history_values(get_distinct_options("_VoteHistory", "_VoteHistory")) if "V4A" in columns else []
    options["mib_applied_vals"] = get_distinct_options("_MIBApplied", "_MIBApplied")
    options["mib_ballot_vals"] = get_distinct_options("_MIBBallot", "_MIBBallot")
    options["mb_perm_vals"] = get_distinct_options("_MBPerm", "_MBPerm")

    con = get_conn()
    age_min, age_max = con.execute(
        "SELECT min(_AgeNum), max(_AgeNum) FROM voters WHERE _Status = 'A' AND _AgeNum IS NOT NULL"
    ).fetchone()
    score_min, score_max = con.execute(
        "SELECT min(_MBScore), max(_MBScore) FROM voters WHERE _Status = 'A' AND _MBScore IS NOT NULL"
    ).fetchone()
    options["age_min"] = int(age_min) if age_min is not None else None
    options["age_max"] = int(age_max) if age_max is not None else None
    options["mb_score_min"] = float(score_min) if score_min is not None else None
    options["mb_score_max"] = float(score_max) if score_max is not None else None
    return options

def query_metrics(active, columns):
    con = get_conn()
    where_sql, params = current_filter_clause(active, columns)
    return con.execute(
        f"""
        SELECT
            count(*) AS voters,
            (
                count(DISTINCT _HouseholdKey) FILTER (WHERE _HouseholdKey IS NOT NULL AND _HouseholdKey <> '')
                + count(*) FILTER (WHERE _HouseholdKey IS NULL OR _HouseholdKey = '')
            ) AS households,
            sum(CASE WHEN _HasEmail THEN 1 ELSE 0 END) AS emails,
            sum(CASE WHEN _HasLandline THEN 1 ELSE 0 END) AS landlines,
            sum(CASE WHEN _HasMobile THEN 1 ELSE 0 END) AS mobiles,
            count(DISTINCT {quote_ident("County")}) FILTER (WHERE {quote_ident("County")} IS NOT NULL) AS unique_counties,
            count(DISTINCT {quote_ident("Precinct")}) FILTER (WHERE {quote_ident("Precinct")} IS NOT NULL) AS unique_precincts
        FROM voters
        {where_sql}
        """,
        params,
    ).df().iloc[0].to_dict()

def query_chart(active, columns, group_expr, label, not_blank=True):
    con = get_conn()
    where_sql, params = current_filter_clause(active, columns)
    extra = f" AND {group_expr} IS NOT NULL AND cast({group_expr} as varchar) <> ''" if not_blank else ""
    return con.execute(
        f"""
        SELECT {group_expr} AS "{label}", count(*) AS "Count"
        FROM voters
        {where_sql}
        {extra}
        GROUP BY 1
        ORDER BY 2 DESC, 1
        """,
        params,
    ).df()

def query_area_summary(active, columns, area_col):
    con = get_conn()
    where_sql, params = current_filter_clause(active, columns)
    return con.execute(
        f"""
        SELECT
            coalesce(cast({quote_ident(area_col)} as varchar), '(Blank)') AS "{area_col}",
            count(*) AS Individuals,
            (
                count(DISTINCT _HouseholdKey) FILTER (WHERE _HouseholdKey IS NOT NULL AND _HouseholdKey <> '')
                + count(*) FILTER (WHERE _HouseholdKey IS NULL OR _HouseholdKey = '')
            ) AS Households
        FROM voters
        {where_sql}
        GROUP BY 1
        ORDER BY Individuals DESC, 1
        """,
        params,
    ).df()

def fmt_pct(v: float) -> str:
    rounded = round(v, 1)
    return f"{int(rounded)}%" if float(rounded).is_integer() else f"{rounded:.1f}%"

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


def normalize_export_text(val):
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if s.lower() in {"nan", "none"}:
        return ""
    return s


def normalize_numeric_string(val):
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if s.lower() in {"nan", "none", ""}:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    return s

def clean_zip_value(val):
    s = normalize_numeric_string(val)
    if not s:
        return ""
    digits = re.sub(r"\D", "", s)
    if len(digits) == 9:
        return f"{digits[:5]}-{digits[5:]}"
    if len(digits) >= 5:
        return digits[:5]
    return digits

def clean_phone_value(val):
    s = normalize_numeric_string(val)
    if not s:
        return ""
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits

def safe_group_series(group: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name not in group.columns:
        return pd.Series([""] * len(group), index=group.index, dtype="object")
    data = group[column_name]
    if isinstance(data, pd.DataFrame):
        data = data.iloc[:, 0]
    return data.fillna("").astype(str).str.strip()

def vote_history_sort_key(value: str):
    s = normalize_export_text(value).upper()
    digits = re.findall(r"\d+", s)
    if digits:
        return (0, int(digits[0]), s)
    return (1, 9999, s)

def ordered_vote_history_values(values):
    cleaned = [normalize_export_text(v) for v in values if normalize_export_text(v) != ""]
    return sorted(cleaned, key=vote_history_sort_key)

def build_household_mail_name(group: pd.DataFrame) -> str:
    names = safe_group_series(group, "Name")
    names = [x for x in names.tolist() if x]
    if len(names) == 0:
        return ""
    if len(names) == 1:
        return names[0]

    last_names = safe_group_series(group, "LastName")
    unique_last = sorted({x for x in last_names.tolist() if x}, key=lambda x: x.lower())
    if len(unique_last) == 1:
        return f"{unique_last[0]} Household"

    full_names = []
    seen = set()
    for name in names:
        key = name.lower()
        if key not in seen:
            full_names.append(name)
            seen.add(key)

    if len(full_names) == 2:
        return f"{full_names[0]} and {full_names[1]}"
    if len(full_names) == 3:
        return f"{full_names[0]}, {full_names[1]} and {full_names[2]}"
    if len(full_names) > 3:
        return f"{full_names[0]}, {full_names[1]} and Family"

    return "Current Resident"

def full_name_from_row(row):
    parts = [
        normalize_export_text(row.get("FirstName", "")),
        normalize_export_text(row.get("MiddleName", "")),
        normalize_export_text(row.get("LastName", "")),
        normalize_export_text(row.get("NameSuffix", "")),
    ]
    return " ".join([p for p in parts if p]).strip()

def build_address_line1_row(row):
    parts = [
        normalize_export_text(row.get("House Number", "")),
        normalize_export_text(row.get("Street Name", "")),
    ]
    line1 = " ".join([p for p in parts if p]).strip()
    apt = normalize_export_text(row.get("Apartment Number", ""))
    if apt:
        line1 = f"{line1} Apt {apt}".strip()
    return line1

def first_existing_detail(columns, candidates):
    lower_map = {str(c).strip().lower(): c for c in columns}
    for col in candidates:
        if col in columns:
            return col
        hit = lower_map.get(str(col).strip().lower())
        if hit is not None:
            return hit
    return None

@st.cache_data(show_spinner=True)
def ensure_detail_shards():
    manifest = load_manifest()
    local_paths = []
    for shard in manifest["detail"]["shards"]:
        key = shard["key"]
        local_path = LOCAL_ROOT / key
        download_public_object(key, local_path)
        local_paths.append(str(local_path))
    return local_paths, manifest

def build_detail_export_sql(detail_paths, active_filters):
    paths_sql = "[" + ", ".join(sql_string_literal(p) for p in detail_paths) + "]"
    columns = get_conn().execute(f"DESCRIBE SELECT * FROM read_parquet({paths_sql})").df()["column_name"].tolist()

    q = quote_ident
    status_col = first_existing_detail(columns, ["VoterStatus", "voterstatus"])
    party_col = first_existing_detail(columns, ["Party"])
    gender_col = first_existing_detail(columns, ["Gender", "Sex"])
    age_col = first_existing_detail(columns, ["Age"])
    hh_col = first_existing_detail(columns, ["HH_ID"])
    email_col = first_existing_detail(columns, ["Email"])
    landline_col = first_existing_detail(columns, ["Landline"])
    mobile_col = first_existing_detail(columns, ["Mobile"])
    vote_hist_col = first_existing_detail(columns, ["V4A"])

    exprs = ["*"]
    if status_col:
        exprs.append(f"upper(trim(coalesce(cast({q(status_col)} as varchar), ''))) as _Status")
    else:
        exprs.append("'A' as _Status")

    if party_col:
        exprs.append(
            f"""case
                when upper(trim(coalesce(cast({q(party_col)} as varchar), ''))) in ('', 'NONE', 'NAN', 'U') then 'O'
                else upper(trim(cast({q(party_col)} as varchar)))
            end as _PartyNorm"""
        )
    else:
        exprs.append("'O' as _PartyNorm")

    if gender_col:
        exprs.append(
            f"""case
                when upper(trim(coalesce(cast({q(gender_col)} as varchar), ''))) in ('', 'NONE', 'NAN') then 'U'
                else upper(trim(cast({q(gender_col)} as varchar)))
            end as _Gender"""
        )
    else:
        exprs.append("'U' as _Gender")

    if age_col:
        exprs.append(f"try_cast({q(age_col)} as double) as _AgeNum")
    else:
        exprs.append("NULL::DOUBLE as _AgeNum")

    for alias, src in [("_HasEmail", email_col), ("_HasLandline", landline_col), ("_HasMobile", mobile_col)]:
        if src:
            exprs.append(
                f"""case
                    when trim(coalesce(cast({q(src)} as varchar), '')) in ('', 'None', 'NONE', 'nan', 'NAN') then false
                    else true
                end as {alias}"""
            )
        else:
            exprs.append(f"false as {alias}")

    if vote_hist_col:
        exprs.append(f"upper(trim(coalesce(cast({q(vote_hist_col)} as varchar), ''))) as _VoteHistory")
    else:
        exprs.append("'' as _VoteHistory")

    if mib_applied_col:
        exprs.append(f"upper(trim(coalesce(cast({q(mib_applied_col)} as varchar), ''))) as _MIBApplied")
    else:
        exprs.append("'' as _MIBApplied")

    if mib_ballot_col:
        exprs.append(f"upper(trim(coalesce(cast({q(mib_ballot_col)} as varchar), ''))) as _MIBBallot")
    else:
        exprs.append("'' as _MIBBallot")

    if mb_score_col:
        exprs.append(f"try_cast({q(mb_score_col)} as double) as _MBScore")
    else:
        exprs.append("NULL::DOUBLE as _MBScore")

    if mb_perm_col:
        exprs.append(f"""case
            when upper(trim(coalesce(cast({q(mb_perm_col)} as varchar), ''))) in ('TRUE', 'T', 'YES', 'Y', '1') then 'Y'
            when upper(trim(coalesce(cast({q(mb_perm_col)} as varchar), ''))) in ('FALSE', 'F', 'NO', 'N', '0') then 'N'
            else ''
        end as _MBPerm""")
    else:
        exprs.append("'' as _MBPerm")

    if hh_col:
        exprs.append(f"nullif(trim(coalesce(cast({q(hh_col)} as varchar), '')), '') as _HouseholdKey")
    else:
        exprs.append("NULL::VARCHAR as _HouseholdKey")

    where_sql, params = current_filter_clause(active_filters, columns)
    sql = "SELECT\n    " + ",\n    ".join(exprs) + f"\nFROM read_parquet({paths_sql})\n{where_sql}"
    return sql, params

def fetch_filtered_detail(active_filters):
    detail_paths, _ = ensure_detail_shards()
    sql, params = build_detail_export_sql(detail_paths, active_filters)
    return get_conn().execute(sql, params).df()

def build_filtered_csv_export(active_filters):
    df = fetch_filtered_detail(active_filters).copy()
    for col in ["Zip", "ZIP", "ZipCode", "ZIPCODE", "MailingZip", "Mailing Zip", "MailZip"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_zip_value)
    for col in ["PrimaryPhone", "Mobile", "Landline"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_phone_value)
    return df

def build_texting_export(active_filters):
    df = fetch_filtered_detail(active_filters).copy()
    if df.empty:
        return pd.DataFrame(columns=["Name", "Mobile", "Party", "Age", "County", "Precinct"])
    df["Name"] = df.apply(full_name_from_row, axis=1)
    mobile_col = "Mobile" if "Mobile" in df.columns else None
    if mobile_col is None:
        df["MobileClean"] = ""
    else:
        df["MobileClean"] = df[mobile_col].apply(clean_phone_value)
    cols = [c for c in ["Name", "Party", "Age", "County", "Precinct"] if c in df.columns]
    out = df[cols].copy()
    out.insert(1, "Mobile", df["MobileClean"])
    out = out[out["Mobile"].astype(str).str.strip() != ""]
    return out.reset_index(drop=True)

def build_mail_export(active_filters, householded=False):
    df = fetch_filtered_detail(active_filters).copy()
    if df.empty:
        return pd.DataFrame(columns=["Name", "Address1", "City", "State", "Zip", "Party", "Age"])
    df["Name"] = df.apply(full_name_from_row, axis=1)
    df["Address1"] = df.apply(build_address_line1_row, axis=1)
    city_col = first_existing_detail(df.columns.tolist(), ["MailingCity", "Mailing City", "City", "MailCity"])
    state_col = first_existing_detail(df.columns.tolist(), ["MailingState", "Mailing State", "State", "MailState"])
    zip_col = first_existing_detail(df.columns.tolist(), ["MailingZip", "Mailing Zip", "ZIP", "Zip", "ZipCode", "ZIPCODE", "MailZip"])
    df["CityOut"] = df[city_col].apply(normalize_export_text) if city_col else ""
    df["StateOut"] = df[state_col].apply(normalize_export_text) if state_col else ""
    df["ZipOut"] = df[zip_col].apply(clean_zip_value) if zip_col else ""

    out = df[["Name", "Address1", "CityOut", "StateOut", "ZipOut"]].copy()
    if "Party" in df.columns:
        out["Party"] = df["Party"]
    if "Age" in df.columns:
        out["Age"] = df["Age"]
    out = out.rename(columns={"CityOut": "City", "StateOut": "State", "ZipOut": "Zip"})

    if householded:
        key = "_HouseholdKey" if "_HouseholdKey" in df.columns else None
        temp = pd.concat([df.reset_index(drop=True), out.reset_index(drop=True)], axis=1)

        address_text = temp["Address1"].apply(normalize_export_text)
        zip_text = temp["Zip"].apply(clean_zip_value)
        fallback_key = address_text + "|" + zip_text

        if key:
            base_key = temp[key].apply(normalize_export_text)
            grp_key = base_key.where(base_key != "", fallback_key)
        else:
            grp_key = fallback_key

        temp["_grp"] = grp_key

        grouped_rows = []
        for _, grp in temp.sort_values(by=["_grp", "Name"]).groupby("_grp", dropna=False):
            first = grp.iloc[0].copy()
            first["Name"] = build_household_mail_name(grp)
            grouped_rows.append(first[out.columns].to_dict())
        out = pd.DataFrame(grouped_rows, columns=out.columns)

    return out.reset_index(drop=True)

def dataframe_to_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8")


def safe_int_from_value(value):
    s = normalize_export_text(value)
    if not s:
        return 10**9
    digits = re.findall(r"\d+", s)
    if digits:
        try:
            return int(digits[0])
        except Exception:
            return 10**9
    return 10**9

def normalize_checkbox_flag(value) -> bool:
    s = normalize_export_text(value).upper()
    return s in {"Y", "YES", "TRUE", "T", "1"}

def normalize_mb_perm_value(value) -> str:
    s = normalize_export_text(value).upper()
    if s in {"Y", "YES", "TRUE", "T", "1"}:
        return "Y"
    if s in {"N", "NO", "FALSE", "F", "0"}:
        return "N"
    return ""

def choose_phone_and_type(row) -> tuple[str, str]:
    mobile = clean_phone_value(row.get("Mobile", ""))
    landline = clean_phone_value(row.get("Landline", ""))
    primary = clean_phone_value(row.get("PrimaryPhone", ""))
    if mobile:
        return mobile, "m"
    if landline:
        return landline, "l"
    if primary:
        return primary, "p"
    return "", ""

def format_phone_display(row) -> str:
    number, kind = choose_phone_and_type(row)
    if not number:
        return ""
    if len(number) == 10:
        number = f"({number[:3]}) {number[3:6]}-{number[6:]}"
    elif len(number) == 7:
        number = f"{number[:3]}-{number[3:]}"
    return f"{number} ({kind})" if kind else number

def compact_filter_summary(active_filters: dict) -> list[str]:
    lines = []
    geo_cols = ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"]
    for col in geo_cols:
        vals = active_filters.get(col, [])
        if vals:
            sample = ", ".join(map(str, vals[:3]))
            if len(vals) > 3:
                sample += f" (+{len(vals)-3} more)"
            lines.append(f"{col}: {sample}")
    mapping = [
        ("party_pick", "Party"), ("hh_party_pick", "Household Party"),
        ("calc_party_pick", "Calculated Party"), ("gender_pick", "Gender"),
        ("age_range_pick", "Age Range"), ("vote_history_pick", "Vote History"),
        ("mib_applied_pick", "Mail Ballot App"), ("mib_ballot_pick", "Mail Ballot Vote"),
        ("mb_perm_pick", "MB Perm"),
    ]
    for key, label in mapping:
        vals = active_filters.get(key, [])
        if vals:
            sample = ", ".join(map(str, vals[:4]))
            if len(vals) > 4:
                sample += f" (+{len(vals)-4} more)"
            lines.append(f"{label}: {sample}")
    if active_filters.get("age_slider") is not None:
        lo, hi = active_filters["age_slider"]
        lines.append(f"Age: {lo} to {hi}")
    if active_filters.get("mb_score_slider") is not None:
        lo, hi = active_filters["mb_score_slider"]
        lines.append(f"MB Probability Score: {lo:.2f} to {hi:.2f}")
    if active_filters.get("new_reg_months", 0):
        lines.append(f"Newly Registered: within last {int(active_filters['new_reg_months'])} months")
    for key, label in [("has_email", "Email"), ("has_landline", "Landline"), ("has_mobile", "Mobile")]:
        val = active_filters.get(key, "All")
        if val and val != "All":
            lines.append(f"{label}: {val}")
    return lines or ["All active voters in current loaded geography / filter scope"]

def build_street_report_dataframe(active_filters):
    df = fetch_filtered_detail(active_filters).copy()
    if df.empty:
        return df

    if "Full Name" not in df.columns:
        df["Full Name"] = df.apply(full_name_from_row, axis=1)
    else:
        df["Full Name"] = df["Full Name"].apply(normalize_name_value)
        blank_mask = df["Full Name"].astype(str).str.strip() == ""
        if blank_mask.any():
            df.loc[blank_mask, "Full Name"] = df[blank_mask].apply(full_name_from_row, axis=1)

    df["Street"] = df.get("Street Name", "").apply(normalize_address_value) if "Street Name" in df.columns else ""
    df["HouseNumSort"] = df.get("House Number", "").apply(safe_int_from_value) if "House Number" in df.columns else 10**9
    df["House Number Text"] = df.get("House Number", "").apply(normalize_export_text) if "House Number" in df.columns else ""
    df["Apartment Text"] = df.get("Apartment Number", "").apply(normalize_export_text) if "Apartment Number" in df.columns else ""
    df["Address Line1"] = df.apply(build_address_line1_row, axis=1).apply(normalize_address_value)
    df["Phone Display"] = df.apply(format_phone_display, axis=1)
    df["Party Display"] = df.get("Party", "").apply(normalize_export_text) if "Party" in df.columns else ""
    if "Sex" in df.columns:
        df["Sex Display"] = df["Sex"].apply(normalize_export_text)
    elif "Gender" in df.columns:
        df["Sex Display"] = df["Gender"].apply(normalize_export_text)
    else:
        df["Sex Display"] = ""
    df["Age Display"] = df.get("Age", "").apply(normalize_export_text) if "Age" in df.columns else ""
    df["MB Perm Display"] = ""
    for col in ["MB_PERM", "MB_Perm", "MB_Pern"]:
        if col in df.columns:
            df["MB Perm Display"] = df[col].apply(normalize_mb_perm_value)
            break

    # checkbox columns
    for col in ["F", "A", "U", "Yard Sign"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_checkbox_flag)
        else:
            df[col] = False
    df["NH"] = False

    if "Precinct" not in df.columns:
        df["Precinct"] = "(No Precinct)"
    df["Precinct"] = df["Precinct"].apply(normalize_export_text).replace("", "(No Precinct)")
    df["Street"] = df["Street"].replace("", "(No Street)")
    df["AptSort"] = df["Apartment Text"].str.upper()

    sort_cols = ["Precinct", "Street", "HouseNumSort", "Apartment Text", "Full Name"]
    df = df.sort_values(by=sort_cols, kind="stable").reset_index(drop=True)
    return df

def count_households_from_report_df(frame: pd.DataFrame) -> int:
    if len(frame) == 0:
        return 0
    keys = frame["Address Line1"].astype(str).fillna("") + "|" + frame["Precinct"].astype(str).fillna("")
    return keys.nunique()

def build_precinct_counts_for_report(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame(columns=["Precinct", "Individuals", "Households"])
    temp = report_df.copy()
    temp["_hh"] = temp["Precinct"].astype(str).fillna("") + "|" + temp["Address Line1"].astype(str).fillna("")
    out = (
        temp.groupby("Precinct", dropna=False)
        .agg(Individuals=("Precinct", "size"), Households=("_hh", "nunique"))
        .reset_index()
        .sort_values("Precinct")
        .reset_index(drop=True)
    )
    return out

def _draw_checkbox(c, x, y_center, size=7):
    c.rect(x, y_center - size/2, size, size, stroke=1, fill=0)

def _draw_footer(c, page_num, total_pages, printed_date):
    width, _ = letter
    c.setStrokeColor(colors.HexColor("#d7d1d1"))
    c.line(36, 30, width - 36, 30)
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.black)
    c.drawString(36, 18, f"{page_num} of {total_pages}")
    c.drawRightString(width - 36, 18, f"Updated: {printed_date}")

def _draw_powered_by(c, page_title=None):
    width, height = letter
    right_x = width - 36
    if page_title:
        c.setFont("Helvetica-Bold", 15)
        c.drawString(36, height - 40, page_title)
    c.setFont("Helvetica-Bold", 8)
    c.drawRightString(right_x, height - 22, "Powered By")
    if TSS_LOGO.exists():
        try:
            img = ImageReader(str(TSS_LOGO))
            c.drawImage(img, right_x - 55, height - 44, width=50, height=18, preserveAspectRatio=True, mask='auto')
        except Exception:
            pass

def _draw_cover_page(c, total_pages, printed_date, report_df, active_filters):
    width, height = letter
    if CC_LOGO.exists():
        try:
            img = ImageReader(str(CC_LOGO))
            c.drawImage(img, 36, height - 85, width=130, height=36, preserveAspectRatio=True, mask='auto')
        except Exception:
            pass
    _draw_powered_by(c)

    county_text = ", ".join(active_filters.get("County", [])[:3]) if active_filters.get("County") else "Selected Area"
    c.setFont("Helvetica-Bold", 22)
    c.drawString(36, height - 120, county_text or "Selected Area")
    c.setFont("Helvetica", 12)
    c.drawString(36, height - 140, printed_date)

    title_bits = []
    if active_filters.get("party_pick"):
        title_bits.append("/".join(active_filters["party_pick"]) + " voters")
    else:
        title_bits.append("Filtered voters")
    if active_filters.get("County"):
        title_bits.append("in " + ", ".join(active_filters["County"][:2]))
    c.setFont("Helvetica-Bold", 16)
    c.drawString(36, height - 170, " ".join(title_bits).strip())

    individuals = len(report_df)
    households = count_households_from_report_df(report_df)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, height - 195, f"Individuals: {individuals:,}  Households: {households:,}")

    c.setFont("Helvetica-Bold", 13)
    c.drawString(36, height - 230, "Filters / Area Description")
    y = height - 248
    c.setFont("Helvetica", 10)
    for line in compact_filter_summary(active_filters):
        wrapped = []
        line = str(line)
        while len(line) > 95:
            cut = line.rfind(" ", 0, 95)
            if cut == -1:
                cut = 95
            wrapped.append(line[:cut])
            line = line[cut:].strip()
        wrapped.append(line)
        for part in wrapped:
            if y < 70:
                break
            c.drawString(48, y, "- " + part)
            y -= 13

    _draw_footer(c, 1, total_pages, printed_date)
    c.showPage()

def _draw_counts_summary_pages(c, page_num, total_pages, printed_date, precinct_counts):
    width, height = letter
    rows_per_page = 40
    pages_used = 0
    for start in range(0, len(precinct_counts), rows_per_page):
        chunk = precinct_counts.iloc[start:start+rows_per_page]
        _draw_powered_by(c, "Precinct Counts Summary")
        c.setFont("Helvetica-Bold", 16)
        c.drawString(36, height - 48, "Precinct Counts Summary")

        top = height - 78
        c.setFillColor(colors.HexColor("#f2f4f7"))
        c.rect(36, top - 18, width - 72, 18, stroke=0, fill=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(42, top - 13, "Precinct")
        c.drawRightString(width - 100, top - 13, "Individuals")
        c.drawRightString(width - 42, top - 13, "Households")

        y = top - 32
        c.setFont("Helvetica", 10)
        for _, row in chunk.iterrows():
            precinct = str(row["Precinct"])
            individuals = f'{int(row["Individuals"]):,}'
            households = f'{int(row["Households"]):,}'
            c.drawString(42, y, precinct[:58])
            c.drawRightString(width - 100, y, individuals)
            c.drawRightString(width - 42, y, households)
            y -= 14
        _draw_footer(c, page_num + pages_used, total_pages, printed_date)
        pages_used += 1
        c.showPage()
    return pages_used

def _build_precinct_page_descriptors(report_df):
    pages = []
    max_y = 698
    bottom_limit = 46
    current = None

    for precinct, precinct_df in report_df.groupby("Precinct", sort=False):
        first_page = True
        y = max_y
        current_rows = []

        def flush_page(cont_flag):
            nonlocal current_rows, y, first_page
            if current_rows:
                pages.append({"precinct": precinct, "cont": cont_flag, "rows": current_rows[:]})
                current_rows = []
                y = max_y

        for street, street_df in precinct_df.groupby("Street", sort=False):
            if y < bottom_limit + 18:
                flush_page(not first_page)
                first_page = False
            current_rows.append(("street", street))
            y -= 14

            for address, addr_df in street_df.groupby("Address Line1", sort=False):
                needed = 12 + len(addr_df) * 12
                if y - needed < bottom_limit:
                    flush_page(not first_page)
                    first_page = False
                    current_rows.append(("street", street))
                    y -= 14
                current_rows.append(("address", address))
                y -= 12
                for _, row in addr_df.iterrows():
                    current_rows.append(("person", row.to_dict()))
                    y -= 12

        flush_page(not first_page)
    return pages

def _draw_precinct_page(c, descriptor, page_num, total_pages, printed_date):
    width, height = letter
    precinct = descriptor["precinct"]
    cont = descriptor["cont"]
    title = precinct + (" (cont)" if cont else "")
    _draw_powered_by(c, title)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(36, height - 40, title)

    # header row
    top = height - 64
    c.setFillColor(colors.HexColor("#f2f4f7"))
    c.rect(36, top - 14, width - 72, 16, stroke=0, fill=1)
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(40, top - 10, "Full Name")
    c.drawString(230, top - 10, "Phone")
    c.drawString(346, top - 10, "Party")
    c.drawString(382, top - 10, "Sex")
    c.drawString(410, top - 10, "Age")
    c.drawString(438, top - 10, "F")
    c.drawString(454, top - 10, "A")
    c.drawString(470, top - 10, "U")
    c.drawString(486, top - 10, "NH")
    c.drawString(505, top - 10, "Yard Sign")
    c.drawString(560, top - 10, "MB_Perm")

    y = top - 26
    c.setFont("Helvetica", 9)
    last_kind = None
    for kind, payload in descriptor["rows"]:
        if kind == "street":
            c.setFont("Helvetica-Bold", 9)
            c.drawString(40, y, str(payload))
            c.setFont("Helvetica", 9)
            y -= 0
        elif kind == "address":
            c.drawString(52, y, str(payload))
        elif kind == "person":
            row = payload
            c.drawString(64, y, str(row.get("Full Name", ""))[:34])
            c.drawString(230, y, str(row.get("Phone Display", ""))[:18])
            c.drawCentredString(356, y, str(row.get("Party Display", ""))[:2])
            c.drawCentredString(390, y, str(row.get("Sex Display", ""))[:1])
            c.drawCentredString(420, y, str(row.get("Age Display", ""))[:3])
            _draw_checkbox(c, 434, y+3)
            _draw_checkbox(c, 450, y+3)
            _draw_checkbox(c, 466, y+3)
            _draw_checkbox(c, 482, y+3)
            _draw_checkbox(c, 502, y+3)
            c.drawCentredString(576, y, str(row.get("MB Perm Display", ""))[:1])
        y -= 12

    _draw_footer(c, page_num, total_pages, printed_date)

def build_street_list_pdf_bytes(active_filters):
    report_df = build_street_report_dataframe(active_filters)
    if report_df.empty:
        return b""
    precinct_counts = build_precinct_counts_for_report(report_df)
    precinct_pages = _build_precinct_page_descriptors(report_df)
    counts_pages = max(1, math.ceil(len(precinct_counts) / 40)) if len(precinct_counts) else 1
    total_pages = 1 + counts_pages + len(precinct_pages)
    printed_date = datetime.now().strftime("%m/%d/%Y")

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)

    _draw_cover_page(c, total_pages, printed_date, report_df, active_filters)
    page_no = 2
    used = _draw_counts_summary_pages(c, page_no, total_pages, printed_date, precinct_counts)
    page_no += used

    for i, desc in enumerate(precinct_pages):
        bookmark = f"precinct_{i}_{re.sub(r'[^A-Za-z0-9]+', '_', desc['precinct'])}"
        if not desc["cont"]:
            c.bookmarkPage(bookmark)
            c.addOutlineEntry(desc["precinct"], bookmark, level=0, closed=False)
        _draw_precinct_page(c, desc, page_no, total_pages, printed_date)
        c.showPage()
        page_no += 1

    c.save()
    return buffer.getvalue()

cc_logo_uri = img_to_data_uri(CC_LOGO)
tss_logo_uri = img_to_data_uri(TSS_LOGO)

header_html = f"""
<div class="top-shell">
  <div class="brand-grid">
    <div class="brand-left">{f'<img class="logo-cc" src="{cc_logo_uri}"/>' if cc_logo_uri else ''}</div>
    <div class="brand-center">
      <div class="brand-title">Candidate Connect</div>
      <div class="brand-sub">DuckDB + R2 Pass 1: Fast counts and filters on R2 index shards</div>
      <div class="brand-status">Storage: Cloudflare R2 Public Read &nbsp;&nbsp;|&nbsp;&nbsp; Last Local Manifest: {file_modified_text(LOCAL_MANIFEST)}</div>
    </div>
    <div class="brand-right"><div class="powered-by">Powered By</div>{f'<img class="logo-tss" src="{tss_logo_uri}"/>' if tss_logo_uri else ''}</div>
  </div>
</div>
"""
st.markdown(header_html, unsafe_allow_html=True)

if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if "filters_applied" not in st.session_state:
    st.session_state.filters_applied = False
if "active_filters" not in st.session_state:
    st.session_state.active_filters = {}
if "columns" not in st.session_state:
    st.session_state.columns = []
if "options" not in st.session_state:
    st.session_state.options = {}

with st.sidebar:
    st.header("Filters")
    st.markdown('<div class="sidebar-note">This version uses public HTTPS downloads from Cloudflare R2 instead of boto3. Make sure your R2 bucket is Public Read enabled.</div>', unsafe_allow_html=True)

    if not st.session_state.data_loaded:
        if st.button("Load Voter Data", use_container_width=True, type="primary"):
            with st.spinner("Downloading manifest and opening R2 index shards..."):
                local_paths, _manifest = ensure_index_shards()
                st.session_state.columns = prepare_db(local_paths)
                st.session_state.options = get_basic_options(st.session_state.columns)
                st.session_state.data_loaded = True
                st.session_state.filters_applied = False
            st.rerun()
    else:
        st.success("R2 index shards loaded")

        cols = st.session_state.columns
        opts = st.session_state.options

        with st.form("filter_form", clear_on_submit=False):
            with st.expander("Geography", expanded=False):
                geo_cols = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in cols]
                geo_selections = {}
                for col in geo_cols:
                    geo_selections[col] = st.multiselect(col, opts.get(col, []), default=st.session_state.active_filters.get(col, []))

            with st.expander("Voter Details", expanded=False):
                party_pick = st.multiselect("Party", opts.get("party_vals", []), default=st.session_state.active_filters.get("party_pick", []))
                hh_party_pick = st.multiselect("Household Party", opts.get("hh_party_vals", []), default=st.session_state.active_filters.get("hh_party_pick", [])) if "HH-Party" in cols else []
                calc_party_pick = st.multiselect("Calculated Party", opts.get("calc_party_vals", []), default=st.session_state.active_filters.get("calc_party_pick", [])) if "CalculatedParty" in cols else []
                gender_pick = st.multiselect("Gender", opts.get("gender_vals", []), default=st.session_state.active_filters.get("gender_pick", []))
                age_range_pick = st.multiselect("Age Range", opts.get("age_range_vals", []), default=st.session_state.active_filters.get("age_range_pick", []))
                age_slider = None
                if opts.get("age_min") is not None and opts.get("age_max") is not None:
                    age_slider = st.slider("Age", opts["age_min"], opts["age_max"], st.session_state.active_filters.get("age_slider", (opts["age_min"], opts["age_max"])))

            with st.expander("Vote History", expanded=False):
                vote_history_vals = opts.get("vote_history_vals", [])
                vote_history_pick = []
                if vote_history_vals:
                    max_index = len(vote_history_vals) - 1
                    default_vote_idx = st.session_state.active_filters.get("vote_history_index_range", (0, max_index))
                    if not isinstance(default_vote_idx, (list, tuple)) or len(default_vote_idx) != 2:
                        default_vote_idx = (0, max_index)
                    default_vote_idx = (
                        max(0, min(int(default_vote_idx[0]), max_index)),
                        max(0, min(int(default_vote_idx[1]), max_index)),
                    )
                    vote_idx_range = st.slider(
                        "Vote History Range",
                        min_value=0,
                        max_value=max_index,
                        value=default_vote_idx,
                        format="%d",
                    )
                    vote_history_pick = vote_history_vals[vote_idx_range[0]: vote_idx_range[1] + 1]
                    if vote_history_pick:
                        st.caption(f"Selected vote history: {vote_history_pick[0]} to {vote_history_pick[-1]}")
                else:
                    vote_idx_range = (0, 0)
                    st.caption("No vote history values found.")

                mib_applied_pick = st.multiselect("Mail Ballot Application Status", opts.get("mib_applied_vals", []), default=st.session_state.active_filters.get("mib_applied_pick", []))
                mib_ballot_pick = st.multiselect("Mail Ballot Vote Status", opts.get("mib_ballot_vals", []), default=st.session_state.active_filters.get("mib_ballot_pick", []))
                mb_perm_pick = st.multiselect("MB Perm", opts.get("mb_perm_vals", []), default=st.session_state.active_filters.get("mb_perm_pick", []))

                mb_score_slider = None
                if opts.get("mb_score_min") is not None and opts.get("mb_score_max") is not None:
                    lo = float(opts["mb_score_min"])
                    hi = float(opts["mb_score_max"])
                    default_score = st.session_state.active_filters.get("mb_score_slider", (lo, hi))
                    if not isinstance(default_score, (list, tuple)) or len(default_score) != 2:
                        default_score = (lo, hi)
                    mb_score_slider = st.slider(
                        "MB Probability Score",
                        min_value=lo,
                        max_value=hi,
                        value=(float(default_score[0]), float(default_score[1])),
                    )

                new_reg_months = st.slider(
                    "Newly Registered (within last N months; 0 = all)",
                    min_value=0,
                    max_value=24,
                    value=st.session_state.active_filters.get("new_reg_months", 0),
                    step=1,
                )

            with st.expander("Contact Filters", expanded=False):
                email_opts = ["All", "Has Email", "No Email"]
                landline_opts = ["All", "Has Landline", "No Landline"]
                mobile_opts = ["All", "Has Mobile", "No Mobile"]
                has_email = st.selectbox("Email", email_opts, index=email_opts.index(st.session_state.active_filters.get("has_email", "All")))
                has_landline = st.selectbox("Landline", landline_opts, index=landline_opts.index(st.session_state.active_filters.get("has_landline", "All")))
                has_mobile = st.selectbox("Mobile", mobile_opts, index=mobile_opts.index(st.session_state.active_filters.get("has_mobile", "All")))

            st.caption("Counts stay at zero until you click Apply Filters.")
            cols2 = st.columns(2)
            apply_filters = cols2[0].form_submit_button("Apply Filters", use_container_width=True, type="primary")
            clear_filters = cols2[1].form_submit_button("Clear Filters", use_container_width=True)

        if clear_filters:
            st.session_state.active_filters = {}
            st.session_state.filters_applied = False
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
                "vote_history_pick": vote_history_pick,
                "has_email": has_email,
                "has_landline": has_landline,
                "has_mobile": has_mobile,
            }
            st.session_state.filters_applied = True
            st.rerun()

if not st.session_state.data_loaded:
    zeros = [("Voters", "0"), ("Households", "0"), ("Emails", "0"), ("Landlines", "0"), ("Mobiles", "0"), ("Unique Counties", "0"), ("Unique Precincts", "0")]
    metric_cols = st.columns(7, gap="small")
    for col, (label, value) in zip(metric_cols, zeros):
        with col:
            st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)
    divider()
    st.markdown('<div class="section-card empty-shell"><div class="small-header">Ready to load</div><div class="tiny-muted">Click <strong>Load Voter Data</strong> in the sidebar to open the R2 index shards with DuckDB.</div></div>', unsafe_allow_html=True)
    st.stop()

if not st.session_state.filters_applied:
    zeros = [("Voters", "0"), ("Households", "0"), ("Emails", "0"), ("Landlines", "0"), ("Mobiles", "0"), ("Unique Counties", "0"), ("Unique Precincts", "0")]
    metric_cols = st.columns(7, gap="small")
    for col, (label, value) in zip(metric_cols, zeros):
        with col:
            st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)
    divider()
    st.markdown('<div class="section-card empty-shell"><div class="small-header">Filters are loaded</div><div class="tiny-muted">Choose your filters in the sidebar and click <strong>Apply Filters</strong> to run counts and charts.</div></div>', unsafe_allow_html=True)
    st.stop()

active = st.session_state.active_filters
columns = st.session_state.columns

with st.spinner("Running DuckDB queries..."):
    metrics = query_metrics(active, columns)
    party_df = query_chart(active, columns, "_PartyNorm", "Party")
    gender_df = query_chart(active, columns, "_Gender", "Gender")
    age_df = query_chart(active, columns, "_AgeRange", "Age Range")
    area_choices = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in columns]

metric_cols = st.columns(7, gap="small")
metric_values = [
    ("Voters", f"{int(metrics.get('voters') or 0):,}"),
    ("Households", f"{int(metrics.get('households') or 0):,}"),
    ("Emails", f"{int(metrics.get('emails') or 0):,}"),
    ("Landlines", f"{int(metrics.get('landlines') or 0):,}"),
    ("Mobiles", f"{int(metrics.get('mobiles') or 0):,}"),
    ("Unique Counties", f"{int(metrics.get('unique_counties') or 0):,}"),
    ("Unique Precincts", f"{int(metrics.get('unique_precincts') or 0):,}"),
]
for col, (label, value) in zip(metric_cols, metric_values):
    with col:
        st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)

divider()

chart_cols = st.columns(3, gap="medium")
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
if area_choices:
    selected_area = st.selectbox("Area", area_choices, label_visibility="collapsed")
    area_df = query_area_summary(active, columns, selected_area).copy()
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
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="small-header">Exports</div>', unsafe_allow_html=True)
st.caption("Export files are only built when you click the button for that export type.")

mail_mode = st.radio(
    "Mailing Mode",
    ["Not Householded", "Householded"],
    horizontal=True,
    key="mail_mode_radio",
)

exp_cols = st.columns(3, gap="medium")

with exp_cols[0]:
    if st.button("Prepare Filtered CSV", use_container_width=True):
        with st.spinner("Building filtered CSV from detail shards..."):
            export_df = build_filtered_csv_export(active)
            st.session_state["filtered_export_df"] = export_df
    if "filtered_export_df" in st.session_state:
        st.download_button(
            "Download Filtered CSV",
            data=dataframe_to_csv_bytes(st.session_state["filtered_export_df"]),
            file_name="candidate_connect_filtered.csv",
            mime="text/csv",
            use_container_width=True,
        )

with exp_cols[1]:
    if st.button("Prepare Texting CSV", use_container_width=True):
        with st.spinner("Building texting CSV from detail shards..."):
            export_df = build_texting_export(active)
            st.session_state["texting_export_df"] = export_df
    if "texting_export_df" in st.session_state:
        st.download_button(
            "Download Texting CSV",
            data=dataframe_to_csv_bytes(st.session_state["texting_export_df"]),
            file_name="candidate_connect_texting.csv",
            mime="text/csv",
            use_container_width=True,
        )

with exp_cols[2]:
    if st.button("Prepare Mail CSV", use_container_width=True):
        with st.spinner("Building mail CSV from detail shards..."):
            export_df = build_mail_export(active, householded=(mail_mode == "Householded"))
            st.session_state["mail_export_df"] = export_df
            st.session_state["mail_export_mode"] = mail_mode
    if "mail_export_df" in st.session_state:
        suffix = "householded" if st.session_state.get("mail_export_mode") == "Householded" else "individual"
        st.download_button(
            "Download Mail CSV",
            data=dataframe_to_csv_bytes(st.session_state["mail_export_df"]),
            file_name=f"candidate_connect_mail_{suffix}.csv",
            mime="text/csv",
            use_container_width=True,
        )


street_cols = st.columns(2, gap="medium")
with street_cols[0]:
    if st.button("Prepare Street List PDF", use_container_width=True):
        with st.spinner("Building street list PDF from detail shards..."):
            pdf_bytes = build_street_list_pdf_bytes(active)
            st.session_state["street_pdf_bytes"] = pdf_bytes
    if "street_pdf_bytes" in st.session_state:
        st.download_button(
            "Download Street List PDF",
            data=st.session_state["street_pdf_bytes"],
            file_name="candidate_connect_street_list.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

with street_cols[1]:
    st.caption("Street list PDF matches the desktop-style layout: cover, precinct counts summary, precinct sections, footer page counts, and bookmarks.")

st.markdown('</div>', unsafe_allow_html=True)
