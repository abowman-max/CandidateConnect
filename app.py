import json
import os
from pathlib import Path
import base64
import re
import zipfile

import altair as alt
import duckdb
import pandas as pd
import math
import requests
import streamlit as st
import boto3

from io import BytesIO
from datetime import datetime
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter

st.set_page_config(page_title="Candidate Connect", layout="wide")

# R2 public-read setup
R2_BASE = "https://pub-a9e33b718082407cbd85e7b86b0fcb5c.r2.dev"
R2_BUCKET = "candidate-connect-data"

LOCAL_ROOT = Path("/tmp/candidate_connect_r2")
LOCAL_MANIFEST = LOCAL_ROOT / "dataset_manifest.json"

CC_LOGO = Path("candidate_connect_logo.png")
TSS_LOGO = Path("TSS_Logo_Transparent.png")
SAVED_UNIVERSES_PATH = Path("saved_universes.json")
SAVED_UNIVERSES_R2_KEY = "app_state/saved_universes.json"


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

def get_secret_value(*keys, default=None):
    try:
        for key in keys:
            if key in st.secrets:
                return st.secrets[key]
    except Exception:
        pass
    for key in keys:
        val = os.environ.get(key)
        if val not in (None, ""):
            return val
    return default


def get_saved_universe_store_info() -> dict:
    account_id = get_secret_value("R2_ACCOUNT_ID", "CLOUDFLARE_ACCOUNT_ID")
    access_key = get_secret_value("R2_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
    secret_key = get_secret_value("R2_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
    bucket = get_secret_value("R2_BUCKET", "SAVED_UNIVERSES_BUCKET", default=R2_BUCKET)
    endpoint_url = get_secret_value("R2_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3")
    region = get_secret_value("AWS_DEFAULT_REGION", default="auto")

    if not endpoint_url and account_id:
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    ready = all([endpoint_url, access_key, secret_key, bucket])
    return {
        "ready": bool(ready),
        "endpoint_url": endpoint_url,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": bucket,
        "region": region,
    }


def get_saved_universe_store_label() -> str:
    info = get_saved_universe_store_info()
    return "Cloudflare R2" if info.get("ready") else "Local fallback"


def get_saved_universes_r2_client():
    info = get_saved_universe_store_info()
    if not info.get("ready"):
        return None, info
    client = boto3.client(
        "s3",
        endpoint_url=info["endpoint_url"],
        aws_access_key_id=info["access_key"],
        aws_secret_access_key=info["secret_key"],
        region_name=info["region"],
    )
    return client, info


def _load_saved_universes_local() -> dict:
    if not SAVED_UNIVERSES_PATH.exists():
        return {}
    try:
        data = json.loads(SAVED_UNIVERSES_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_saved_universes() -> dict:
    client, info = get_saved_universes_r2_client()
    if client is None:
        return _load_saved_universes_local()
    try:
        obj = client.get_object(Bucket=info["bucket"], Key=SAVED_UNIVERSES_R2_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_saved_universes(data: dict):
    payload = json.dumps(data, indent=2).encode("utf-8")
    client, info = get_saved_universes_r2_client()
    if client is None:
        SAVED_UNIVERSES_PATH.write_bytes(payload)
        return
    client.put_object(
        Bucket=info["bucket"],
        Key=SAVED_UNIVERSES_R2_KEY,
        Body=payload,
        ContentType="application/json",
        CacheControl="no-store",
    )


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
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet({paths_sql}, union_by_name=True)").df()
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
    mb_score_col = first_existing(columns, ["MB_AProp_Score", "MMB_AProp_Score"])
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
        exprs.append(
            f"""coalesce(
                try_strptime(cast({q(reg_col)} as varchar), '%m/%d/%Y'),
                try_strptime(cast({q(reg_col)} as varchar), '%m/%d/%y'),
                try_cast({q(reg_col)} as timestamp)
            ) as _RegistrationDate"""
        )
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
        exprs.append(f"try_cast(regexp_replace(cast({q(mb_score_col)} as varchar), '[^0-9\\.-]', '', 'g') as double) as _MBScore")
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
    vote_history_type = active.get("vote_history_type", "All")
    vote_history_range = active.get("vote_history_range")

    if vote_history_range is not None:
        low, high = vote_history_range

        if vote_history_type == "General" and "V4G" in columns:
            vh_col = '"V4G"'
        elif vote_history_type == "Primary" and "V4P" in columns:
            vh_col = '"V4P"'
        elif "V4A" in columns:
            vh_col = '"V4A"'
        else:
            vh_col = None

        if vh_col:
            where.append(
                f"""
                coalesce(
                    try_cast(nullif(trim(cast({vh_col} as varchar)), '') as integer),
                    0
                ) >= ?
                AND
                coalesce(
                    try_cast(nullif(trim(cast({vh_col} as varchar)), '') as integer),
                    0
                ) <= ?
                """
            )
            params.extend([int(low), int(high)])
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
        if "_RegistrationDate" in columns:
            where.append("_RegistrationDate >= (CURRENT_DATE - (? * INTERVAL '1 month'))")
            params.append(int(active["new_reg_months"]))
        elif "RegistrationDate" in columns:
            where.append("""coalesce(
                try_strptime(cast("RegistrationDate" as varchar), '%m/%d/%Y'),
                try_strptime(cast("RegistrationDate" as varchar), '%m/%d/%y'),
                try_cast("RegistrationDate" as timestamp)
            ) >= (CURRENT_DATE - (? * INTERVAL '1 month'))""")
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
    if has_global_followup_filters(active):
        return _query_metrics_from_detail(active, columns)

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
    if has_global_followup_filters(active):
        return _query_chart_from_detail(active, group_expr, label, not_blank=not_blank)

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
    if has_global_followup_filters(active):
        return _query_area_summary_from_detail(active, area_col)

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



def build_statewide_summary_report_bytes(active_filters, columns):
    con = get_conn()
    where_sql, params = current_filter_clause(active_filters, columns)

    def grouped_summary(group_col: str, label: str):
        if group_col not in columns:
            return pd.DataFrame(columns=[label, "Voters", "Households", "Democrats", "Republicans", "Others", "Male", "Female", "Unknown Gender", "MIB Applied", "MIB Declined", "Did Not Apply", "Not Sent", "Not Voted", "Voted", "Permanent Mail", "Emails", "Mobiles"])
        qcol = quote_ident(group_col)
        return con.execute(
            f"""
            SELECT
                coalesce(cast({qcol} as varchar), '(Blank)') AS "{label}",
                count(*) AS "Voters",
                (
                    count(DISTINCT _HouseholdKey) FILTER (WHERE _HouseholdKey IS NOT NULL AND _HouseholdKey <> '')
                    + count(*) FILTER (WHERE _HouseholdKey IS NULL OR _HouseholdKey = '')
                ) AS "Households",
                sum(CASE WHEN _PartyNorm = 'D' THEN 1 ELSE 0 END) AS "Democrats",
                sum(CASE WHEN _PartyNorm = 'R' THEN 1 ELSE 0 END) AS "Republicans",
                sum(CASE WHEN _PartyNorm NOT IN ('D','R') THEN 1 ELSE 0 END) AS "Others",
                sum(CASE WHEN _Gender = 'M' THEN 1 ELSE 0 END) AS "Male",
                sum(CASE WHEN _Gender = 'F' THEN 1 ELSE 0 END) AS "Female",
                sum(CASE WHEN _Gender NOT IN ('M','F') THEN 1 ELSE 0 END) AS "Unknown Gender",
                sum(CASE WHEN _MIBApplied = 'APP' THEN 1 ELSE 0 END) AS "MIB Applied",
                sum(CASE WHEN _MIBApplied = 'DEC' THEN 1 ELSE 0 END) AS "MIB Declined",
                sum(CASE WHEN _MIBApplied = 'DNA' THEN 1 ELSE 0 END) AS "Did Not Apply",
                sum(CASE WHEN _MIBBallot = 'NS' THEN 1 ELSE 0 END) AS "Not Sent",
                sum(CASE WHEN _MIBBallot = 'NV' THEN 1 ELSE 0 END) AS "Not Voted",
                sum(CASE WHEN _MIBBallot = 'V' THEN 1 ELSE 0 END) AS "Voted",
                sum(CASE WHEN _MBPerm = 'Y' THEN 1 ELSE 0 END) AS "Permanent Mail",
                sum(CASE WHEN _HasEmail THEN 1 ELSE 0 END) AS "Emails",
                sum(CASE WHEN _HasMobile THEN 1 ELSE 0 END) AS "Mobiles"
            FROM voters
            {where_sql}
            GROUP BY 1
            ORDER BY "Voters" DESC, 1
            """,
            params,
        ).df()

    overview = con.execute(
        f"""
        SELECT
            count(*) AS "Voters",
            (
                count(DISTINCT _HouseholdKey) FILTER (WHERE _HouseholdKey IS NOT NULL AND _HouseholdKey <> '')
                + count(*) FILTER (WHERE _HouseholdKey IS NULL OR _HouseholdKey = '')
            ) AS "Households",
            sum(CASE WHEN _PartyNorm = 'D' THEN 1 ELSE 0 END) AS "Democrats",
            sum(CASE WHEN _PartyNorm = 'R' THEN 1 ELSE 0 END) AS "Republicans",
            sum(CASE WHEN _PartyNorm NOT IN ('D','R') THEN 1 ELSE 0 END) AS "Others",
            sum(CASE WHEN _Gender = 'M' THEN 1 ELSE 0 END) AS "Male",
            sum(CASE WHEN _Gender = 'F' THEN 1 ELSE 0 END) AS "Female",
            sum(CASE WHEN _Gender NOT IN ('M','F') THEN 1 ELSE 0 END) AS "Unknown Gender",
            sum(CASE WHEN _MIBApplied = 'APP' THEN 1 ELSE 0 END) AS "MIB Applied",
            sum(CASE WHEN _MIBApplied = 'DEC' THEN 1 ELSE 0 END) AS "MIB Declined",
            sum(CASE WHEN _MIBApplied = 'DNA' THEN 1 ELSE 0 END) AS "Did Not Apply",
            sum(CASE WHEN _MIBBallot = 'NS' THEN 1 ELSE 0 END) AS "Not Sent",
            sum(CASE WHEN _MIBBallot = 'NV' THEN 1 ELSE 0 END) AS "Not Voted",
            sum(CASE WHEN _MIBBallot = 'V' THEN 1 ELSE 0 END) AS "Voted",
            sum(CASE WHEN _MBPerm = 'Y' THEN 1 ELSE 0 END) AS "Permanent Mail",
            sum(CASE WHEN _HasEmail THEN 1 ELSE 0 END) AS "Emails",
            sum(CASE WHEN _HasMobile THEN 1 ELSE 0 END) AS "Mobiles",
            count(DISTINCT "County") FILTER (WHERE "County" IS NOT NULL) AS "Unique Counties",
            count(DISTINCT "Precinct") FILTER (WHERE "Precinct" IS NOT NULL) AS "Unique Precincts"
        FROM voters
        {where_sql}
        """,
        params,
    ).df()

    filter_df = pd.DataFrame({"Applied Universe Filters": build_filter_summary_lines(active_filters)})

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        overview.to_excel(writer, sheet_name="Overview", index=False)
        filter_df.to_excel(writer, sheet_name="Filters", index=False)
        for group_col, label in [("County", "County"), ("USC", "USC"), ("STS", "STS"), ("STH", "STH")]:
            grouped_summary(group_col, label).to_excel(writer, sheet_name=label[:31], index=False)
    return output.getvalue()

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


def safe_int(val) -> int:
    try:
        if val is None:
            return 0
        if isinstance(val, str) and val.strip().lower() in {"", "nan", "none"}:
            return 0
        if pd.isna(val):
            return 0
        return int(float(val))
    except Exception:
        return 0

def get_filtered_voter_count_fast(active_filters, columns) -> int:
    con = get_conn()
    where_sql, params = current_filter_clause(active_filters, columns)
    try:
        row = con.execute(f"SELECT count(*) AS n FROM voters {where_sql}", params).fetchone()
        return safe_int(row[0] if row else 0)
    except Exception:
        return 0

def use_large_filter_mode(active_filters, columns) -> bool:
    try:
        return get_filtered_voter_count_fast(active_filters, columns) >= 500000
    except Exception:
        return False



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


USPS_SUFFIX_MAP = {
    "STREET": "ST", "ST": "ST",
    "ROAD": "RD", "RD": "RD",
    "AVENUE": "AVE", "AVE": "AVE",
    "DRIVE": "DR", "DR": "DR",
    "LANE": "LN", "LN": "LN",
    "COURT": "CT", "CT": "CT",
    "CIRCLE": "CIR", "CIR": "CIR",
    "BOULEVARD": "BLVD", "BLVD": "BLVD",
    "PLACE": "PL", "PL": "PL",
    "TERRACE": "TER", "TER": "TER",
    "PARKWAY": "PKWY", "PKWY": "PKWY",
    "HIGHWAY": "HWY", "HWY": "HWY",
    "MOUNT": "MT", "MT": "MT",
}
STATE_ABBR = {
    "ALABAMA":"AL","ALASKA":"AK","ARIZONA":"AZ","ARKANSAS":"AR","CALIFORNIA":"CA","COLORADO":"CO",
    "CONNECTICUT":"CT","DELAWARE":"DE","FLORIDA":"FL","GEORGIA":"GA","HAWAII":"HI","IDAHO":"ID",
    "ILLINOIS":"IL","INDIANA":"IN","IOWA":"IA","KANSAS":"KS","KENTUCKY":"KY","LOUISIANA":"LA",
    "MAINE":"ME","MARYLAND":"MD","MASSACHUSETTS":"MA","MICHIGAN":"MI","MINNESOTA":"MN","MISSISSIPPI":"MS",
    "MISSOURI":"MO","MONTANA":"MT","NEBRASKA":"NE","NEVADA":"NV","NEW HAMPSHIRE":"NH","NEW JERSEY":"NJ",
    "NEW MEXICO":"NM","NEW YORK":"NY","NORTH CAROLINA":"NC","NORTH DAKOTA":"ND","OHIO":"OH","OKLAHOMA":"OK",
    "OREGON":"OR","PENNSYLVANIA":"PA","RHODE ISLAND":"RI","SOUTH CAROLINA":"SC","SOUTH DAKOTA":"SD",
    "TENNESSEE":"TN","TEXAS":"TX","UTAH":"UT","VERMONT":"VT","VIRGINIA":"VA","WASHINGTON":"WA",
    "WEST VIRGINIA":"WV","WISCONSIN":"WI","WYOMING":"WY","DISTRICT OF COLUMBIA":"DC"
}
NAME_SUFFIXES = {"JR","SR","II","III","IV","V"}

def collapse_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", normalize_export_text(value)).strip()

def proper_case_word(word: str) -> str:
    if not word:
        return ""
    up = word.upper()
    if up in NAME_SUFFIXES:
        return up
    if re.fullmatch(r"[A-Z]\.", up):
        return up
    if "'" in word:
        return "'".join(part.capitalize() if part else "" for part in word.lower().split("'"))
    if "-" in word:
        return "-".join(part.capitalize() if part else "" for part in word.lower().split("-"))
    return word.lower().capitalize()

def normalize_name_value(value: str) -> str:
    s = collapse_spaces(value)
    if not s:
        return ""
    return " ".join(proper_case_word(part) for part in s.split(" "))

def normalize_city_value(value: str) -> str:
    s = collapse_spaces(value)
    if not s:
        return ""
    return " ".join(proper_case_word(part) for part in s.split(" "))

def normalize_state_value(value: str) -> str:
    s = collapse_spaces(value).upper()
    if not s:
        return ""
    if len(s) == 2 and s.isalpha():
        return s
    return STATE_ABBR.get(s, s[:2] if len(s) >= 2 else s)

def normalize_address_value(value: str) -> str:
    s = collapse_spaces(value)
    if not s:
        return ""

    s = re.sub(r"Apartment", "Apt", s, flags=re.IGNORECASE)
    s = re.sub(r"Suite", "Ste", s, flags=re.IGNORECASE)
    s = re.sub(r"Unit", "Unit", s, flags=re.IGNORECASE)

    words = s.split(" ")
    words = [proper_case_word(w) for w in words]

    if words:
        last = re.sub(r"[^A-Za-z]", "", words[-1]).upper()
        if last in USPS_SUFFIX_MAP:
            words[-1] = USPS_SUFFIX_MAP[last].title()

    return " ".join(words)

def normalize_mail_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Name" in out.columns:
        out["Name"] = out["Name"].apply(normalize_name_value)
    if "Address1" in out.columns:
        out["Address1"] = out["Address1"].apply(normalize_address_value)
    if "City" in out.columns:
        out["City"] = out["City"].apply(normalize_city_value)
    if "State" in out.columns:
        out["State"] = out["State"].apply(normalize_state_value)
    if "Zip" in out.columns:
        out["Zip"] = out["Zip"].apply(clean_zip_value)
    return out

def normalize_filtered_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["FirstName", "MiddleName", "LastName", "FullName", "Name", "NameSuffix"]:
        if col in out.columns:
            out[col] = out[col].apply(normalize_name_value)
    for col in ["Street Name", "Address", "Address1", "Mailing Address", "MailAddress"]:
        if col in out.columns:
            out[col] = out[col].apply(normalize_address_value)
    for col in ["City", "MailingCity", "Mailing City", "MailCity"]:
        if col in out.columns:
            out[col] = out[col].apply(normalize_city_value)
    for col in ["State", "MailingState", "Mailing State", "MailState"]:
        if col in out.columns:
            out[col] = out[col].apply(normalize_state_value)
    for col in ["Zip", "ZIP", "ZipCode", "ZIPCODE", "MailingZip", "Mailing Zip", "MailZip"]:
        if col in out.columns:
            out[col] = out[col].apply(clean_zip_value)
    for col in ["PrimaryPhone", "Mobile", "Landline"]:
        if col in out.columns:
            out[col] = out[col].apply(clean_phone_value)
    return out

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
    columns = get_conn().execute(f"DESCRIBE SELECT * FROM read_parquet({paths_sql}, union_by_name=True)").df()["column_name"].tolist()

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
    mib_applied_col = first_existing_detail(columns, ["MIB_Applied"])
    mib_ballot_col = first_existing_detail(columns, ["MIB_BALLOT"])
    mb_score_col = first_existing_detail(columns, ["MB_AProp_Score", "MMB_AProp_Score"])
    mb_perm_col = first_existing_detail(columns, ["MB_PERM", "MB_Perm", "MB_Pern"])

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
        exprs.append(f"try_cast(regexp_replace(cast({q(mb_score_col)} as varchar), '[^0-9\\.-]', '', 'g') as double) as _MBScore")
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
    sql = "SELECT\n    " + ",\n    ".join(exprs) + f"\nFROM read_parquet({paths_sql}, union_by_name=True)\n{where_sql}"
    return sql, params

def fetch_filtered_detail(active_filters):
    detail_paths, _ = ensure_detail_shards()
    sql, params = build_detail_export_sql(detail_paths, active_filters)
    df = get_conn().execute(sql, params).df()
    if has_global_followup_filters(active_filters):
        df = apply_global_followup_filters_df(df, active_filters)
    return df

def build_filtered_csv_export(active_filters):
    df = fetch_filtered_detail(active_filters).copy()
    return normalize_filtered_export_dataframe(df)

def build_texting_export(active_filters):
    df = fetch_filtered_detail(active_filters).copy()
    empty_cols = ["Name", "PA ID Number", "Mobile", "Party", "Age", "County", "Precinct"]
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    df["Name"] = df.apply(full_name_from_row, axis=1)

    mobile_col = first_existing_detail(df.columns.tolist(), ["Mobile", "Cell", "CellPhone", "Cell Phone"])
    if mobile_col is None:
        df["MobileClean"] = ""
    else:
        df["MobileClean"] = df[mobile_col].apply(clean_phone_value)

    pa_id_col = first_existing_detail(
        df.columns.tolist(),
        ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"]
    )
    if pa_id_col is not None:
        df["PA ID Number"] = df[pa_id_col].apply(normalize_numeric_string)
    else:
        df["PA ID Number"] = ""

    cols = [c for c in ["Name", "PA ID Number", "Party", "Age", "County", "Precinct"] if c in df.columns]
    out = df[cols].copy()
    out.insert(2, "Mobile", df["MobileClean"])
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

    export_df = pd.DataFrame({
        "MailName": df["Name"].apply(normalize_export_text),
        "Address1": df["Address1"].apply(normalize_export_text),
        "City": df["CityOut"].apply(normalize_export_text),
        "State": df["StateOut"].apply(normalize_export_text),
        "Zip": df["ZipOut"].apply(clean_zip_value),
    })

    if householded:
        key_name = "_HouseholdKey" if "_HouseholdKey" in df.columns else None
        temp = pd.DataFrame({
            "_BaseName": df["Name"].apply(normalize_export_text),
            "FirstName": safe_group_series(df, "FirstName"),
            "LastName": safe_group_series(df, "LastName"),
            "Address1": export_df["Address1"].apply(normalize_export_text),
            "City": export_df["City"].apply(normalize_export_text),
            "State": export_df["State"].apply(normalize_export_text),
            "Zip": export_df["Zip"].apply(clean_zip_value),
        })

        address_text = temp["Address1"].apply(normalize_export_text)
        zip_text = temp["Zip"].apply(clean_zip_value)
        fallback_key = address_text + "|" + zip_text

        if key_name and key_name in df.columns:
            base_key = safe_group_series(df, key_name)
            grp_key = base_key.where(base_key != "", fallback_key)
        else:
            grp_key = fallback_key

        temp["_grp"] = grp_key.fillna("").astype(str)
        temp["Name"] = temp["_BaseName"]

        grouped_rows = []
        grouped = temp.sort_values(by=["_grp", "_BaseName"]).groupby("_grp", dropna=False, sort=False)
        for _, grp in grouped:
            first_row = grp.iloc[0].copy()
            first_row["MailName"] = build_household_mail_name(grp)
            row = {
                "Name": first_row["MailName"],
                "Address1": normalize_export_text(first_row["Address1"]),
                "City": normalize_export_text(first_row["City"]),
                "State": normalize_export_text(first_row["State"]),
                "Zip": clean_zip_value(first_row["Zip"]),
            }
            grouped_rows.append(row)

        out = pd.DataFrame(grouped_rows)
        cols = ["Name", "Address1", "City", "State", "Zip"]
        out = out[cols]
        out = out.reset_index(drop=True)
        return normalize_mail_dataframe(out)

    out = export_df.rename(columns={"MailName": "Name"})
    cols = ["Name", "Address1", "City", "State", "Zip"]
    out = out[cols]
    out = out.reset_index(drop=True)
    return normalize_mail_dataframe(out)

def dataframe_to_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8")

def sanitize_filename_part(value: str) -> str:
    s = normalize_export_text(value)
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")
    return s or "blank"



def turf_packet_display_name(packet_label: str, turf_id: str) -> str:
    label = normalize_export_text(packet_label)
    turf = normalize_export_text(turf_id) or "Turf"
    return f"{label} - {turf}" if label else turf


def choose_group_value(row, preferred_columns):
    for col in preferred_columns:
        if col in row and normalize_export_text(row.get(col, "")):
            return normalize_export_text(row.get(col, ""))
    return "(Blank)"


def assign_turf_ids(df: pd.DataFrame, mode: str, target_size: int) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_HouseholdKeySafe"] = out.get("_HouseholdKey", "").fillna("").astype(str).str.strip() if "_HouseholdKey" in out.columns else ""
    if "Address1" not in out.columns:
        out["Address1"] = out.apply(build_address_line1_row, axis=1)

    if mode == "By Precinct":
        group_vals = out.apply(lambda r: choose_group_value(r, ["Precinct"]), axis=1)
        out["Turf_Group"] = group_vals
        out["Turf_ID"] = out["Turf_Group"].apply(lambda v: f"Turf_{sanitize_filename_part(v)}")
    elif mode == "By Municipality":
        group_vals = out.apply(lambda r: choose_group_value(r, ["Municipality", "County"]), axis=1)
        out["Turf_Group"] = group_vals
        out["Turf_ID"] = out["Turf_Group"].apply(lambda v: f"Turf_{sanitize_filename_part(v)}")
    else:
        out["_DoorKey"] = out["_HouseholdKeySafe"]
        blank_mask = out["_DoorKey"].eq("")
        out.loc[blank_mask, "_DoorKey"] = out.loc[blank_mask, "Address1"].fillna("").astype(str)

        work = out.copy()
        household_sizes = work.groupby("_DoorKey", dropna=False).size().reset_index(name="_VoterCount")
        household_sizes["_DoorCount"] = 1
        household_sizes["_StreetSort"] = household_sizes["_DoorKey"].astype(str)
        household_sizes = household_sizes.sort_values(["_StreetSort", "_DoorKey"], kind="stable").reset_index(drop=True)

        turf_ids = []
        turf_num = 1
        current_size = 0
        for _, hh in household_sizes.iterrows():
            increment = int(hh["_DoorCount"] if mode == "Target Doors" else hh["_VoterCount"])
            if current_size > 0 and current_size + increment > int(target_size):
                turf_num += 1
                current_size = 0
            turf_ids.append(f"Turf_{turf_num:03d}")
            current_size += increment
        household_sizes["Turf_ID"] = turf_ids
        out = out.merge(household_sizes[["_DoorKey", "Turf_ID"]], on="_DoorKey", how="left")
        out["Turf_Group"] = out["Turf_ID"]

    summary = (
        out.groupby("Turf_ID", dropna=False)
        .agg(
            Voters=("Turf_ID", "size"),
            Households=("_HouseholdKeySafe", lambda s: s.replace("", pd.NA).dropna().nunique() + (s.eq("").sum())),
            Counties=("County", lambda s: ", ".join(sorted({normalize_export_text(v) for v in s if normalize_export_text(v)})[:4])),
            Municipalities=("Municipality", lambda s: ", ".join(sorted({normalize_export_text(v) for v in s if normalize_export_text(v)})[:4])),
            Precincts=("Precinct", lambda s: ", ".join(sorted({normalize_export_text(v) for v in s if normalize_export_text(v)})[:4])),
        )
        .reset_index()
        .sort_values("Turf_ID")
        .reset_index(drop=True)
    )
    out = out.merge(summary[["Turf_ID", "Voters", "Households"]], on="Turf_ID", how="left")
    out = out.rename(columns={"Voters": "Turf_Voters", "Households": "Turf_Households"})
    return out


def build_turf_packet_zip(active_filters, mode: str, target_size: int = 50, volunteer_name: str = "", packet_label: str = "", packet_date: str = "", include_walksheets: bool = True, max_turfs: int = 0):
    df = fetch_filtered_detail(active_filters).copy()
    if df.empty:
        return b""

    volunteer_name = normalize_name_value(volunteer_name)
    packet_label = collapse_spaces(packet_label)
    packet_date = normalize_export_text(packet_date) or datetime.now().strftime("%Y-%m-%d")

    df["Name"] = df.apply(full_name_from_row, axis=1)
    df["Address1"] = df.apply(build_address_line1_row, axis=1)
    city_col = first_existing_detail(df.columns.tolist(), ["MailingCity", "Mailing City", "City", "MailCity"])
    state_col = first_existing_detail(df.columns.tolist(), ["MailingState", "Mailing State", "State", "MailState"])
    zip_col = first_existing_detail(df.columns.tolist(), ["MailingZip", "Mailing Zip", "ZIP", "Zip", "ZipCode", "ZIPCODE", "MailZip"])
    if city_col and "City" not in df.columns:
        df["City"] = df[city_col]
    if state_col and "State" not in df.columns:
        df["State"] = df[state_col]
    if zip_col and "Zip" not in df.columns:
        df["Zip"] = df[zip_col]

    pa_id_col = first_existing_detail(df.columns.tolist(), ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "Voter ID", "VoterID"])
    if pa_id_col and pa_id_col != "PA_ID_Number":
        df["PA_ID_Number"] = df[pa_id_col]
    elif "PA_ID_Number" not in df.columns:
        df["PA_ID_Number"] = ""

    df = assign_turf_ids(df, mode=mode, target_size=target_size)

    export_cols = [c for c in [
        "Turf_ID", "Name", "PA_ID_Number", "Address1", "City", "State", "Zip",
        "County", "Municipality", "Precinct", "Party", "Gender", "Age", "Mobile", "Landline"
    ] if c in df.columns]

    export_df = df[export_cols].copy()
    export_df = normalize_filtered_export_dataframe(export_df)
    if "Zip" in export_df.columns:
        export_df["Zip"] = export_df["Zip"].apply(clean_zip_value)
    if "Mobile" in export_df.columns:
        export_df["Mobile"] = export_df["Mobile"].apply(clean_phone_value)
    if "Landline" in export_df.columns:
        export_df["Landline"] = export_df["Landline"].apply(clean_phone_value)

    export_df.insert(1, "Packet_Label", packet_label)
    export_df.insert(2, "Volunteer_Name", volunteer_name)
    export_df.insert(3, "Packet_Date", packet_date)

    summary_df = (
        df.groupby("Turf_ID", dropna=False)
        .agg(
            Voters=("Turf_ID", "size"),
            Households=("_HouseholdKeySafe", lambda s: s.replace("", pd.NA).dropna().nunique() + (s.eq("").sum())),
            Counties=("County", lambda s: ", ".join(sorted({normalize_export_text(v) for v in s if normalize_export_text(v)})[:4])),
            Municipalities=("Municipality", lambda s: ", ".join(sorted({normalize_export_text(v) for v in s if normalize_export_text(v)})[:4])),
            Precincts=("Precinct", lambda s: ", ".join(sorted({normalize_export_text(v) for v in s if normalize_export_text(v)})[:4])),
        )
        .reset_index()
        .sort_values("Turf_ID")
        .reset_index(drop=True)
    )
    summary_df.insert(1, "Packet_Label", packet_label)
    summary_df.insert(2, "Volunteer_Name", volunteer_name)
    summary_df.insert(3, "Packet_Date", packet_date)

    if int(max_turfs or 0) > 0:
        selected_turfs = summary_df["Turf_ID"].head(int(max_turfs)).tolist()
        df = df[df["Turf_ID"].isin(selected_turfs)].copy()
        summary_df = summary_df[summary_df["Turf_ID"].isin(selected_turfs)].copy()

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("turf_summary.csv", summary_df.to_csv(index=False))
        readme_lines = [
            "Candidate Connect Turf Packets",
            "",
            "This zip contains turf_summary.csv and one CSV per turf.",
            "Walk sheet PDFs are included only when 'CSV + Walk Sheet PDFs' is selected.",
            f"Packet Label: {packet_label or '(none)'}",
            f"Volunteer: {volunteer_name or '(unassigned)'}",
            f"Packet Date: {packet_date}",
            f"Mode: {mode}",
            f"Walk Sheets Included: {'Yes' if include_walksheets else 'No'}",
            f"Turf Limit Applied: {int(max_turfs) if int(max_turfs or 0) > 0 else 'All'}",
        ]
        zf.writestr("README.txt", "\n".join(readme_lines) + "\n")
        for turf_id, turf_df in df.groupby("Turf_ID", sort=True, dropna=False):
            safe_id = sanitize_filename_part(str(turf_id))
            packet_base = sanitize_filename_part(turf_packet_display_name(packet_label, str(turf_id)))
            csv_df = export_df[export_df["Turf_ID"] == turf_id].drop(columns=["Turf_ID"], errors="ignore")
            zf.writestr(f"turf_packets/{packet_base}.csv", csv_df.to_csv(index=False))

            turf_street_df = build_street_list_dataframe_from_detail_df(turf_df.copy())
            summary_row = summary_df[summary_df["Turf_ID"] == turf_id]
            voters = int(summary_row["Voters"].iloc[0]) if not summary_row.empty else len(turf_df)
            households = int(summary_row["Households"].iloc[0]) if not summary_row.empty else 0
            precincts = summary_row["Precincts"].iloc[0] if not summary_row.empty else ""
            title = turf_packet_display_name(packet_label, str(turf_id))
            filter_parts = [f"{voters:,} voters", f"{households:,} households"]
            if normalize_export_text(volunteer_name):
                filter_parts.append(f"Volunteer: {volunteer_name}")
            if normalize_export_text(packet_date):
                filter_parts.append(packet_date)
            if normalize_export_text(precincts):
                filter_parts.append(precincts)
            filter_desc = " | ".join(filter_parts)
            if include_walksheets:
                pdf_bytes = generate_walk_sheet_pdf_from_street_df(turf_street_df, title, filter_desc)
                if pdf_bytes:
                    zf.writestr(f"turf_walksheets/{packet_base}_walksheet.pdf", pdf_bytes)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def normalize_mb_perm_value(val) -> str:
    s = normalize_export_text(val).upper()
    if s in {"TRUE", "T", "YES", "Y", "1"}:
        return "Y"
    if s in {"FALSE", "F", "NO", "N", "0"}:
        return "N"
    return ""

def choose_best_phone(row) -> str:
    mobile = clean_phone_value(row.get("Mobile", ""))
    landline = clean_phone_value(row.get("Landline", ""))
    primary = clean_phone_value(row.get("PrimaryPhone", ""))
    if mobile:
        return f"({mobile[:3]}) {mobile[3:6]}-{mobile[6:]}" + " (m)" if len(mobile) == 10 else mobile + " (m)"
    if landline:
        return f"({landline[:3]}) {landline[3:6]}-{landline[6:]}" + " (l)" if len(landline) == 10 else landline + " (l)"
    if primary:
        return f"({primary[:3]}) {primary[3:6]}-{primary[6:]}" if len(primary) == 10 else primary
    return ""

def parse_house_number(value) -> int:
    s = normalize_export_text(value)
    m = re.search(r"\d+", s)
    return int(m.group()) if m else 0

def parse_apartment_sort(value) -> tuple:
    s = normalize_export_text(value)
    if not s:
        return (0, "", 0)
    m = re.match(r"([A-Za-z]*)(\d*)", s.replace(" ", ""))
    if m:
        prefix, num = m.groups()
        return (1, prefix.upper(), int(num) if num else 0)
    return (1, s.upper(), 0)


def expand_party_label(code: str) -> str:
    mapping = {"R": "Republicans", "D": "Democrats", "O": "Others"}
    return mapping.get(normalize_export_text(code).upper(), normalize_export_text(code))

def expand_mib_application_label(code: str) -> str:
    mapping = {"APP": "Applied", "DEC": "Declined", "DNA": "None", "": "None"}
    return mapping.get(normalize_export_text(code).upper(), normalize_export_text(code).title())

def summarize_vote_history(picks: list[str]) -> str:
    vals = [normalize_export_text(v) for v in picks if normalize_export_text(v)]
    nums = []
    for v in vals:
        m = re.search(r"(\d+)", v)
        if m:
            nums.append(int(m.group(1)))
    if not nums:
        return ", ".join(vals)
    nums = sorted(set(nums))
    if nums == [4]:
        return "All of the last 4"
    if len(nums) == 1:
        return f"{nums[0]} of the last 4"
    return f"{nums[0]}-{nums[-1]} of the last 4"

def selected_area_desc(active_filters: dict) -> str:
    counties = active_filters.get("County", []) or []
    municipalities = active_filters.get("Municipality", []) or []
    if len(counties) > 1:
        return ", ".join(counties)
    if len(counties) == 1 and municipalities:
        if len(municipalities) == 1:
            return municipalities[0]
        return ", ".join(municipalities[:4]) + (" ..." if len(municipalities) > 4 else "")
    if len(counties) == 1:
        return counties[0]
    if municipalities:
        if len(municipalities) == 1:
            return municipalities[0]
        return ", ".join(municipalities[:4]) + (" ..." if len(municipalities) > 4 else "")
    return "Selected Area"


def build_filter_summary_lines(active_filters: dict) -> list[str]:
    lines = []

    municipalities = active_filters.get("Municipality", []) or []
    if municipalities:
        if len(municipalities) == 1:
            lines.append(f"Municipality: Selected precincts in {municipalities[0].title()}")
        else:
            muni_text = ", ".join(m.title() for m in municipalities[:4])
            if len(municipalities) > 4:
                muni_text += " ..."
            lines.append(f"Municipality: Selected precincts in {muni_text}")

    parties = active_filters.get("party_pick", []) or []
    if parties:
        expanded = ", ".join(expand_party_label(p) for p in parties)
        lines.append(f"Party: {expanded}")

    vote_history_type = active_filters.get("vote_history_type", "All")
    vote_history_range = active_filters.get("vote_history_range")
    if vote_history_range is not None:
        lines.append(f"Vote History ({vote_history_type}): {int(vote_history_range[0])}-{int(vote_history_range[1])} of the last 4")

    mib_app = active_filters.get("mib_applied_pick", []) or []
    if mib_app:
        expanded = ", ".join(expand_mib_application_label(v) for v in mib_app)
        lines.append(f"Mail in Ballot Application: {expanded}")

    mib_vote = active_filters.get("mib_ballot_pick", []) or []
    if mib_vote:
        expanded = ", ".join(normalize_export_text(v).title() for v in mib_vote)
        lines.append(f"Mail Ballot Vote Status: {expanded}")

    mb_perm = active_filters.get("mb_perm_pick", []) or []
    if mb_perm:
        expanded = ", ".join("Y" if normalize_export_text(v).upper() == "Y" else "N" for v in mb_perm)
        lines.append(f"MB Perm: {expanded}")

    for key, label in [("County","County"),("Precinct","Precinct"),("USC","USC"),("STS","STS"),("STH","STH"),("School District","School District"),
                       ("hh_party_pick","Household Party"),("calc_party_pick","Calculated Party"),("gender_pick","Gender"),
                       ("age_range_pick","Age Range")]:
        val = active_filters.get(key)
        if isinstance(val, list) and val:
            lines.append(f"{label}: {', '.join(map(str, val[:8]))}" + (" ..." if len(val) > 8 else ""))

    if active_filters.get("new_reg_months", 0):
        lines.append(f"Newly Registered: within last {active_filters['new_reg_months']} month(s)")
    for key, label in [("has_email","Email"),("has_landline","Landline"),("has_mobile","Mobile")]:
        val = active_filters.get(key)
        if val and val != "All":
            lines.append(f"{label}: {val}")
    return lines or ["No additional filters selected"]


def summarize_universe_filters(active_filters: dict) -> str:
    parts = build_filter_summary_lines(active_filters)
    contact_status = normalize_export_text(active_filters.get("contact_status", "All"))
    if contact_status and contact_status != "All":
        parts.append(f"Contact Status: {contact_status}")
    nh_status = normalize_export_text(active_filters.get("global_nh", "All"))
    if nh_status and nh_status != "All":
        parts.append(f"Not Home: {nh_status}")
    follow_up_status = normalize_export_text(active_filters.get("global_follow_up", "All"))
    if follow_up_status and follow_up_status != "All":
        parts.append(f"Follow-Up: {follow_up_status}")
    support_level = normalize_export_text(active_filters.get("global_support_level", "All"))
    if support_level and support_level != "All":
        parts.append(f"Support Level: {support_level}")
    return " | ".join(parts) if parts else "No filters"


def apply_followup_preset(preset_name: str):
    current = dict(st.session_state.get("active_filters", {}) or {})
    current["contact_status"] = "All"
    current["global_nh"] = "All"
    current["global_follow_up"] = "All"
    current["global_support_level"] = "All"

    if preset_name == "Re-Knock List":
        current["global_nh"] = "Yes"
    elif preset_name == "Follow-Up List":
        current["global_follow_up"] = "Yes"
    elif preset_name == "GOTV Supporters":
        current["global_support_level"] = "Strong"
    elif preset_name == "Undecided Persuasion":
        current["global_support_level"] = "Undecided"
    elif preset_name == "Yard Sign Follow-Up":
        current["contact_status"] = "Contacted"

    st.session_state.active_filters = current
    st.session_state.filters_applied = True
    st.rerun()

def get_global_support_level_options() -> list[str]:
    uploaded = st.session_state.get("walk_results_df")
    if isinstance(uploaded, pd.DataFrame) and not uploaded.empty and "Support Level" in uploaded.columns:
        vals = sorted({normalize_export_text(v) for v in uploaded["Support Level"].tolist() if normalize_export_text(v)})
        return ["All"] + vals
    return ["All", "Strong", "Lean", "Undecided", "Oppose"]

def has_global_followup_filters(active_filters: dict) -> bool:
    if not isinstance(active_filters, dict):
        return False
    return any(
        normalize_export_text(active_filters.get(key, "All")) not in {"", "All"}
        for key in ["contact_status", "global_nh", "global_follow_up", "global_support_level"]
    )

def apply_global_followup_filters_df(df: pd.DataFrame, active_filters: dict) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out = merge_uploaded_street_results_into_detail_df(out)
    out = merge_uploaded_walk_results_into_detail_df(out)

    for field in ["F", "A", "U", "NH", "Yard Sign", "Notes", "Contacted", "Result", "Support Level", "Follow-Up", "Walk Notes"]:
        if field not in out.columns:
            out[field] = ""

    street_contact_mask = (
        out["F"].astype(str).str.strip().ne("") |
        out["A"].astype(str).str.strip().ne("") |
        out["U"].astype(str).str.strip().ne("") |
        out["NH"].astype(str).str.strip().ne("") |
        out["Yard Sign"].astype(str).str.strip().ne("") |
        out["Notes"].astype(str).str.strip().ne("")
    )
    walk_contact_mask = (
        out["Contacted"].astype(str).str.strip().ne("") |
        out["Result"].astype(str).str.strip().ne("") |
        out["Support Level"].astype(str).str.strip().ne("") |
        out["Follow-Up"].astype(str).str.strip().ne("") |
        out["Walk Notes"].astype(str).str.strip().ne("")
    )
    contact_mask = street_contact_mask | walk_contact_mask

    contact_status = normalize_export_text(active_filters.get("contact_status", "All"))
    if contact_status == "Contacted":
        out = out[contact_mask]
    elif contact_status == "Not Contacted":
        out = out[~contact_mask]

    nh_status = normalize_export_text(active_filters.get("global_nh", "All"))
    nh_mask = (
        out["NH"].astype(str).str.strip().ne("") |
        out["Result"].astype(str).str.upper().str.replace(" ", "", regex=False).isin(["NOTHOME", "NH"])
    )
    if nh_status == "Yes":
        out = out[nh_mask]
    elif nh_status == "No":
        out = out[~nh_mask]

    follow_up_status = normalize_export_text(active_filters.get("global_follow_up", "All"))
    follow_up_mask = out["Follow-Up"].astype(str).str.strip().ne("")
    if follow_up_status == "Yes":
        out = out[follow_up_mask]
    elif follow_up_status == "No":
        out = out[~follow_up_mask]

    support_level = normalize_export_text(active_filters.get("global_support_level", "All"))
    if support_level and support_level != "All":
        out = out[
            out["Support Level"].astype(str).str.strip().str.casefold() == support_level.casefold()
        ]

    return out


def query_dashboard_followup_stats(active_filters: dict) -> dict:
    if use_large_filter_mode(active_filters, columns):
        return {
            "contacted_pct": 0,
            "nh_pct": 0,
            "followup_pct": 0,
            "strong_pct": 0,
            "undecided_pct": 0,
            "contacted_count": 0,
            "nh_count": 0,
            "followup_count": 0,
            "strong_count": 0,
            "undecided_count": 0,
            "large_mode": True,
        }

    df = fetch_filtered_detail(active_filters)
    if df is None or df.empty:
        return {
            "contacted_pct": 0,
            "nh_pct": 0,
            "followup_pct": 0,
            "strong_pct": 0,
            "undecided_pct": 0,
            "contacted_count": 0,
            "nh_count": 0,
            "followup_count": 0,
            "strong_count": 0,
            "undecided_count": 0,
            "large_mode": False,
        }

    df = merge_uploaded_street_results_into_detail_df(df)
    df = merge_uploaded_walk_results_into_detail_df(df)

    for field in ["F", "A", "U", "NH", "Yard Sign", "Notes", "Contacted", "Result", "Support Level", "Follow-Up", "Walk Notes"]:
        if field not in df.columns:
            df[field] = ""

    total = max(len(df), 1)

    street_contact_mask = (
        df["F"].astype(str).str.strip().ne("") |
        df["A"].astype(str).str.strip().ne("") |
        df["U"].astype(str).str.strip().ne("") |
        df["NH"].astype(str).str.strip().ne("") |
        df["Yard Sign"].astype(str).str.strip().ne("") |
        df["Notes"].astype(str).str.strip().ne("")
    )
    walk_contact_mask = (
        df["Contacted"].astype(str).str.strip().ne("") |
        df["Result"].astype(str).str.strip().ne("") |
        df["Support Level"].astype(str).str.strip().ne("") |
        df["Follow-Up"].astype(str).str.strip().ne("") |
        df["Walk Notes"].astype(str).str.strip().ne("")
    )
    contacted_mask = street_contact_mask | walk_contact_mask

    nh_mask = (
        df["NH"].astype(str).str.strip().ne("") |
        df["Result"].astype(str).str.upper().str.replace(" ", "", regex=False).isin(["NOTHOME", "NH"])
    )
    followup_mask = df["Follow-Up"].astype(str).str.strip().ne("")
    support_series = df["Support Level"].astype(str).str.strip().str.casefold()
    strong_mask = support_series.eq("strong")
    undecided_mask = support_series.eq("undecided")

    def pct(mask):
        return round((int(mask.sum()) / total) * 100)

    return {
        "contacted_pct": pct(contacted_mask),
        "nh_pct": pct(nh_mask),
        "followup_pct": pct(followup_mask),
        "strong_pct": pct(strong_mask),
        "undecided_pct": pct(undecided_mask),
        "contacted_count": int(contacted_mask.sum()),
        "nh_count": int(nh_mask.sum()),
        "followup_count": int(followup_mask.sum()),
        "strong_count": int(strong_mask.sum()),
        "undecided_count": int(undecided_mask.sum()),
        "large_mode": False,
    }

def _query_metrics_from_detail(active_filters, columns):
    df = fetch_filtered_detail(active_filters)
    if df is None or df.empty:
        return {
            "voters": 0,
            "households": 0,
            "emails": 0,
            "landlines": 0,
            "mobiles": 0,
            "unique_counties": 0,
            "unique_precincts": 0,
        }

    hh = df["_HouseholdKey"].fillna("").astype(str) if "_HouseholdKey" in df.columns else pd.Series([""] * len(df))
    households = int(hh.replace("", pd.NA).dropna().nunique() + hh.eq("").sum())
    return {
        "voters": int(len(df)),
        "households": households,
        "emails": int(df.get("_HasEmail", pd.Series([False] * len(df))).fillna(False).astype(bool).sum()),
        "landlines": int(df.get("_HasLandline", pd.Series([False] * len(df))).fillna(False).astype(bool).sum()),
        "mobiles": int(df.get("_HasMobile", pd.Series([False] * len(df))).fillna(False).astype(bool).sum()),
        "unique_counties": int(df["County"].fillna("").astype(str).replace("", pd.NA).dropna().nunique()) if "County" in df.columns else 0,
        "unique_precincts": int(df["Precinct"].fillna("").astype(str).replace("", pd.NA).dropna().nunique()) if "Precinct" in df.columns else 0,
    }

def _query_chart_from_detail(active_filters, group_expr, label, not_blank=True):
    df = fetch_filtered_detail(active_filters)
    if df is None or df.empty:
        return pd.DataFrame(columns=[label, "Count"])

    series_name = None
    if group_expr in df.columns:
        series_name = group_expr
    elif group_expr == "_PartyNorm" and "_PartyNorm" in df.columns:
        series_name = "_PartyNorm"
    elif group_expr == "_Gender" and "_Gender" in df.columns:
        series_name = "_Gender"
    elif group_expr == "_AgeRange" and "_AgeRange" in df.columns:
        series_name = "_AgeRange"

    if series_name is None:
        return pd.DataFrame(columns=[label, "Count"])

    ser = df[series_name]
    if not_blank:
        ser = ser[ser.fillna("").astype(str).str.strip() != ""]
    out = ser.fillna("(Blank)").astype(str).value_counts(dropna=False).reset_index()
    out.columns = [label, "Count"]
    return out

def _query_area_summary_from_detail(active_filters, area_col):
    df = fetch_filtered_detail(active_filters)
    if df is None or df.empty or area_col not in df.columns:
        return pd.DataFrame(columns=[area_col, "Individuals", "Households"])

    temp = df.copy()
    temp[area_col] = temp[area_col].fillna("(Blank)").astype(str)
    hh = temp["_HouseholdKey"].fillna("").astype(str) if "_HouseholdKey" in temp.columns else pd.Series([""] * len(temp))
    temp["_HouseholdKeySafe"] = hh
    rows = []
    for area_val, grp in temp.groupby(area_col, dropna=False):
        grp_hh = grp["_HouseholdKeySafe"]
        households = int(grp_hh.replace("", pd.NA).dropna().nunique() + grp_hh.eq("").sum())
        rows.append({
            area_col: area_val if normalize_export_text(area_val) else "(Blank)",
            "Individuals": int(len(grp)),
            "Households": households,
        })
    out = pd.DataFrame(rows).sort_values(["Individuals", area_col], ascending=[False, True]).reset_index(drop=True)
    return out



def get_street_results_template_csv_bytes():
    template_df = pd.DataFrame(columns=["PA ID Number", "F", "A", "U", "NH", "Yard Sign", "Notes"])
    return template_df.to_csv(index=False).encode("utf-8")


def get_street_results_sheet_bytes(active_filters):
    street_df = build_street_list_dataframe(active_filters)
    street_df = apply_uploaded_street_result_filters(street_df)

    export_cols = [
        "Precinct", "StreetGroup", "AddressLine", "FullName", "Phone", "Party", "Sex", "Age",
        "PA ID Number", "F", "A", "U", "NH", "Yard Sign", "Notes"
    ]
    for col in export_cols:
        if col not in street_df.columns:
            street_df[col] = ""

    export_df = street_df[export_cols].copy().rename(columns={
        "StreetGroup": "Street",
        "AddressLine": "Address",
        "FullName": "Name",
        "Sex": "Gender",
    })

    wb = Workbook()
    ws = wb.active
    ws.title = "Street List Tracking"

    ws["A1"] = "Candidate Connect Street List Tracking Sheet"
    ws["A2"] = f"Generated {datetime.now().strftime('%Y-%m-%d %I:%M %p')}"
    ws["A3"] = "Enter X in F, A, U, NH, and Yard Sign. Use Notes for anything important from the candidate's conversation."
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(export_df.columns))
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=len(export_df.columns))
    ws.merge_cells(start_row=3, start_column=1, end_row=3, end_column=len(export_df.columns))
    ws["A1"].font = Font(bold=True, size=14, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor="7A1523")
    ws["A1"].alignment = Alignment(horizontal="center")
    ws["A2"].font = Font(italic=True, size=10)
    ws["A3"].font = Font(size=10)

    headers = export_df.columns.tolist()
    header_row = 5
    thin = Side(style="thin", color="D7B7BC")
    header_fill = PatternFill("solid", fgColor="9F2032")
    check_fill = PatternFill("solid", fgColor="F9E8EA")

    for c, header in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=c, value=header)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for r, row in enumerate(export_df.itertuples(index=False), start=header_row + 1):
        for c, value in enumerate(row, start=1):
            cell = ws.cell(row=r, column=c, value=value)
            cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)
            header = headers[c - 1]
            if header in {"F", "A", "U", "NH", "Yard Sign"}:
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.fill = check_fill
                cell.font = Font(bold=True)
            elif header == "Notes":
                cell.alignment = Alignment(wrap_text=True, vertical="top")
            else:
                cell.alignment = Alignment(vertical="center")

    widths = {
        "Precinct": 18, "Street": 24, "Address": 14, "Name": 24, "Phone": 16, "Party": 8,
        "Gender": 8, "Age": 8, "PA ID Number": 16, "F": 5, "A": 5, "U": 5, "NH": 6,
        "Yard Sign": 10, "Notes": 28
    }
    for c, header in enumerate(headers, start=1):
        ws.column_dimensions[get_column_letter(c)].width = widths.get(header, 14)

    ws.freeze_panes = "A6"
    ws.auto_filter.ref = f"A{header_row}:{get_column_letter(len(headers))}{max(header_row, ws.max_row)}"
    for r in range(header_row + 1, ws.max_row + 1):
        ws.row_dimensions[r].height = 18

    out = BytesIO()
    wb.save(out)
    return out.getvalue()

def _normalized_col_lookup(columns):
    lookup = {}
    for col in columns:
        key = re.sub(r"[^a-z0-9]+", "", str(col).strip().lower())
        if key and key not in lookup:
            lookup[key] = col
    return lookup

def _find_uploaded_results_column(columns, candidates):
    lookup = _normalized_col_lookup(columns)
    for candidate in candidates:
        key = re.sub(r"[^a-z0-9]+", "", str(candidate).strip().lower())
        if key in lookup:
            return lookup[key]
    return None

def normalize_tracking_mark(val):
    s = normalize_export_text(val).upper()
    return "X" if s in {"X", "Y", "YES", "TRUE", "T", "1", "CHECK", "CHECKED"} else ""

def standardize_uploaded_street_results(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["PA ID Number", "F", "A", "U", "NH", "Yard Sign", "Notes"])

    pa_id_col = _find_uploaded_results_column(
        df.columns.tolist(),
        ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"]
    )
    if pa_id_col is None:
        return pd.DataFrame(columns=["PA ID Number", "F", "A", "U", "NH", "Yard Sign", "Notes"])

    out = pd.DataFrame()
    out["PA ID Number"] = df[pa_id_col].apply(normalize_numeric_string)
    field_map = {
        "F": ["F"],
        "A": ["A"],
        "U": ["U"],
        "NH": ["NH", "Not Home", "NotHome"],
        "Yard Sign": ["Yard Sign", "YardSign", "Sign", "Yard"],
        "Notes": ["Notes", "Note", "Comments", "Comment"],
    }
    for field, candidates in field_map.items():
        col = _find_uploaded_results_column(df.columns.tolist(), candidates)
        if col is None:
            out[field] = ""
        elif field == "Notes":
            out[field] = df[col].apply(normalize_export_text)
        else:
            out[field] = df[col].apply(normalize_tracking_mark)

    out = out[out["PA ID Number"].astype(str).str.strip() != ""].copy()
    out = out.drop_duplicates(subset=["PA ID Number"], keep="last").reset_index(drop=True)
    return out

def merge_uploaded_street_results_into_detail_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    merged = df.copy()
    uploaded = st.session_state.get("street_results_df")
    if not isinstance(uploaded, pd.DataFrame) or uploaded.empty:
        for field in ["F", "A", "U", "NH", "Yard Sign", "Notes"]:
            if field not in merged.columns:
                merged[field] = ""
        return merged

    pa_id_col = first_existing_detail(
        merged.columns.tolist(),
        ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"]
    )
    if pa_id_col is None:
        for field in ["F", "A", "U", "NH", "Yard Sign", "Notes"]:
            if field not in merged.columns:
                merged[field] = ""
        return merged

    merged["PA ID Number"] = merged[pa_id_col].apply(normalize_numeric_string)
    merge_cols = ["PA ID Number", "F", "A", "U", "NH", "Yard Sign", "Notes"]
    merged = merged.merge(uploaded[merge_cols], on="PA ID Number", how="left")
    for field in ["F", "A", "U", "NH", "Yard Sign", "Notes"]:
        merged[field] = merged[field].fillna("").astype(str)
    return merged

def apply_uploaded_street_result_filters(street_df: pd.DataFrame) -> pd.DataFrame:
    if street_df is None or street_df.empty:
        return street_df

    filters = st.session_state.get("street_results_filters", {}) or {}
    out = street_df.copy()
    for field in ["F", "A", "U", "NH", "Yard Sign"]:
        mode = normalize_export_text(filters.get(field, "All"))
        if mode == "Marked":
            out = out[out[field].astype(str).str.strip() != ""]
        elif mode == "Unmarked":
            out = out[out[field].astype(str).str.strip() == ""]
    return out


def get_walk_sheet_tracking_template_csv_bytes():
    template_df = pd.DataFrame(columns=["PA ID Number", "Contacted", "Result", "Support Level", "Follow-Up", "Notes"])
    return template_df.to_csv(index=False).encode("utf-8")

def normalize_walk_result_value(val):
    return normalize_export_text(val).title()

def standardize_uploaded_walk_results(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["PA ID Number", "Contacted", "Result", "Support Level", "Follow-Up", "Notes"])

    pa_id_col = _find_uploaded_results_column(
        df.columns.tolist(),
        ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"]
    )
    if pa_id_col is None:
        return pd.DataFrame(columns=["PA ID Number", "Contacted", "Result", "Support Level", "Follow-Up", "Notes"])

    out = pd.DataFrame()
    out["PA ID Number"] = df[pa_id_col].apply(normalize_numeric_string)

    field_map = {
        "Contacted": ["Contacted", "Contact", "C"],
        "Result": ["Result", "Outcome", "Canvass Result"],
        "Support Level": ["Support Level", "Support", "SupportLevel"],
        "Follow-Up": ["Follow-Up", "Follow Up", "Followup", "F"],
        "Notes": ["Notes", "Note", "Comments", "Comment"],
    }

    for field, candidates in field_map.items():
        col = _find_uploaded_results_column(df.columns.tolist(), candidates)
        if col is None:
            out[field] = ""
        elif field in {"Contacted", "Follow-Up"}:
            out[field] = df[col].apply(normalize_tracking_mark)
        elif field == "Notes":
            out[field] = df[col].apply(normalize_export_text)
        else:
            out[field] = df[col].apply(normalize_walk_result_value)

    out = out[out["PA ID Number"].astype(str).str.strip() != ""].copy()
    out = out.drop_duplicates(subset=["PA ID Number"], keep="last").reset_index(drop=True)
    return out

def merge_uploaded_walk_results_into_detail_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    merged = df.copy()
    uploaded = st.session_state.get("walk_results_df")
    if not isinstance(uploaded, pd.DataFrame) or uploaded.empty:
        for field in ["Contacted", "Result", "Support Level", "Follow-Up", "Walk Notes"]:
            if field not in merged.columns:
                merged[field] = ""
        return merged

    pa_id_col = first_existing_detail(
        merged.columns.tolist(),
        ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"]
    )
    if pa_id_col is None:
        for field in ["Contacted", "Result", "Support Level", "Follow-Up", "Walk Notes"]:
            if field not in merged.columns:
                merged[field] = ""
        return merged

    merged["PA ID Number"] = merged[pa_id_col].apply(normalize_numeric_string)
    merge_cols = ["PA ID Number", "Contacted", "Result", "Support Level", "Follow-Up", "Notes"]
    uploaded_for_merge = uploaded[merge_cols].rename(columns={"Notes": "_UploadedWalkNotes"})
    merged = merged.merge(uploaded_for_merge, on="PA ID Number", how="left")
    merged["Walk Notes"] = merged["_UploadedWalkNotes"].fillna("").astype(str) if "_UploadedWalkNotes" in merged.columns else ""
    if "_UploadedWalkNotes" in merged.columns:
        merged = merged.drop(columns=["_UploadedWalkNotes"])
    for field in ["Contacted", "Result", "Support Level", "Follow-Up", "Walk Notes"]:
        if field not in merged.columns:
            merged[field] = ""
        merged[field] = merged[field].fillna("").astype(str)
    return merged

def apply_uploaded_walk_result_filters(street_df: pd.DataFrame) -> pd.DataFrame:
    if street_df is None or street_df.empty:
        return street_df

    filters = st.session_state.get("walk_results_filters", {}) or {}
    out = street_df.copy()

    for field in ["Contacted", "Follow-Up"]:
        mode = normalize_export_text(filters.get(field, "All"))
        if mode == "Marked":
            out = out[out[field].astype(str).str.strip() != ""]
        elif mode == "Unmarked":
            out = out[out[field].astype(str).str.strip() == ""]

    not_home_mode = normalize_export_text(filters.get("Not Home", "All"))
    result_upper = out["Result"].astype(str).str.upper().str.replace(" ", "", regex=False)
    if not_home_mode == "Marked":
        out = out[result_upper.isin(["NOTHOME", "NH"])]
    elif not_home_mode == "Unmarked":
        out = out[~result_upper.isin(["NOTHOME", "NH"])]

    support_level = normalize_export_text(filters.get("Support Level", "All"))
    if support_level and support_level != "All":
        out = out[out["Support Level"].astype(str).str.strip().str.casefold() == support_level.casefold()]

    return out

def build_walk_sheet_tracking_excel_bytes(active_filters):
    street_df = build_street_list_dataframe(active_filters).copy()
    street_df = apply_uploaded_walk_result_filters(street_df)
    if street_df.empty:
        export_df = pd.DataFrame(columns=[
            "Precinct", "Street", "Address", "Name", "Phone", "Party", "Gender", "Age",
            "PA ID Number", "Contacted", "Result", "Support Level", "Follow-Up", "Notes"
        ])
    else:
        export_df = pd.DataFrame({
            "Precinct": street_df["Precinct"].apply(normalize_export_text),
            "Street": street_df["StreetGroup"].apply(normalize_export_text),
            "Address": street_df["AddressLine"].apply(normalize_export_text),
            "Name": street_df["FullName"].apply(normalize_export_text),
            "Phone": street_df["Phone"].apply(normalize_export_text),
            "Party": street_df["Party"].apply(normalize_export_text),
            "Gender": street_df["Sex"].apply(normalize_export_text),
            "Age": street_df["Age"].apply(normalize_export_text),
            "PA ID Number": street_df["PA ID Number"].apply(normalize_numeric_string),
            "Contacted": street_df["Contacted"].apply(normalize_export_text) if "Contacted" in street_df.columns else pd.Series([""] * len(street_df)),
            "Result": street_df["Result"].apply(normalize_export_text) if "Result" in street_df.columns else pd.Series([""] * len(street_df)),
            "Support Level": street_df["Support Level"].apply(normalize_export_text) if "Support Level" in street_df.columns else pd.Series([""] * len(street_df)),
            "Follow-Up": street_df["Follow-Up"].apply(normalize_export_text) if "Follow-Up" in street_df.columns else pd.Series([""] * len(street_df)),
            "Notes": street_df.get("Walk Notes", pd.Series([""] * len(street_df))).apply(normalize_export_text),
        })

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, sheet_name="Walk Sheet Tracking", index=False, startrow=4)
        workbook = writer.book
        worksheet = writer.sheets["Walk Sheet Tracking"]

        title_font = Font(bold=True, size=14, color="7A1523")
        sub_font = Font(italic=True, size=10, color="555555")
        header_fill = PatternFill(fill_type="solid", fgColor="7A1523")
        header_font = Font(bold=True, color="FFFFFF")
        thin_side = Side(style="thin", color="C9B0B4")
        box_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)
        box_fill = PatternFill(fill_type="solid", fgColor="F8EDED")
        note_fill = PatternFill(fill_type="solid", fgColor="FFF9F9")
        center_align = Alignment(horizontal="center", vertical="center")
        wrap_align = Alignment(vertical="top", wrap_text=True)

        worksheet["A1"] = "Candidate Connect Walk Sheet Tracking Sheet"
        worksheet["A1"].font = title_font
        worksheet["A2"] = f"Generated: {datetime.now().strftime('%m/%d/%Y %I:%M %p')}"
        worksheet["A2"].font = sub_font
        worksheet["A3"] = "Enter X in Contacted or Follow-Up, type Not Home or another result in Result, and fill Support Level / Notes as needed."
        worksheet["A3"].font = sub_font

        for cell in worksheet[5]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align

        width_map = {
            "A": 14, "B": 22, "C": 16, "D": 28, "E": 18, "F": 8, "G": 9, "H": 8,
            "I": 15, "J": 11, "K": 16, "L": 16, "M": 11, "N": 28
        }
        for col_letter, width in width_map.items():
            worksheet.column_dimensions[col_letter].width = width

        max_row = worksheet.max_row
        for row in range(6, max_row + 1):
            for col_letter in ["J", "M"]:
                cell = worksheet[f"{col_letter}{row}"]
                cell.border = box_border
                cell.fill = box_fill
                cell.alignment = center_align
            for col_letter in ["K", "L", "N"]:
                cell = worksheet[f"{col_letter}{row}"]
                cell.border = box_border
                cell.fill = note_fill
                cell.alignment = wrap_align

        worksheet.freeze_panes = "A6"

    return output.getvalue()

def build_street_list_dataframe(active_filters):
    df = fetch_filtered_detail(active_filters).copy()
    df = merge_uploaded_street_results_into_detail_df(df)
    df = merge_uploaded_walk_results_into_detail_df(df)
    if df.empty:
        return pd.DataFrame(columns=[
            "Precinct","StreetGroup","AddressLine","FullName","Phone","Party","Sex","Age","PA ID Number",
            "F","A","U","NH","Yard Sign","Notes","Contacted","Result","Support Level","Follow-Up","Walk Notes","MB_Perm","HouseNumSort","AptSort"
        ])

    precinct_col = first_existing_detail(df.columns.tolist(), ["Precinct"])
    street_col = first_existing_detail(df.columns.tolist(), ["Street Name"])
    house_col = first_existing_detail(df.columns.tolist(), ["House Number"])
    apt_col = first_existing_detail(df.columns.tolist(), ["Apartment Number"])
    sex_col = first_existing_detail(df.columns.tolist(), ["Gender", "Sex"])
    age_col = first_existing_detail(df.columns.tolist(), ["Age"])
    party_col = first_existing_detail(df.columns.tolist(), ["Party"])
    pa_id_col = first_existing_detail(df.columns.tolist(), ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"])
    mb_perm_col = first_existing_detail(df.columns.tolist(), ["MB_PERM", "MB_Perm", "MB_Pern"])

    out = pd.DataFrame()
    out["Precinct"] = df[precinct_col].apply(normalize_export_text) if precinct_col else ""
    out["StreetGroup"] = df[street_col].apply(normalize_address_value) if street_col else ""
    house_vals = df[house_col].apply(normalize_export_text) if house_col else pd.Series([""] * len(df))
    apt_vals = df[apt_col].apply(normalize_export_text) if apt_col else pd.Series([""] * len(df))
    out["AddressLine"] = house_vals
    out.loc[apt_vals != "", "AddressLine"] = out.loc[apt_vals != "", "AddressLine"] + " Apt " + apt_vals[apt_vals != ""]
    out["AddressLine"] = out["AddressLine"].apply(collapse_spaces).apply(normalize_address_value)
    out["FullName"] = df.apply(full_name_from_row, axis=1).apply(normalize_name_value)
    out["Phone"] = df.apply(choose_best_phone, axis=1)
    out["Party"] = df[party_col].apply(normalize_export_text) if party_col else ""
    out["Sex"] = df[sex_col].apply(normalize_export_text) if sex_col else ""
    out["Age"] = df[age_col].apply(lambda v: normalize_numeric_string(v)) if age_col else ""
    out["PA ID Number"] = df[pa_id_col].apply(normalize_numeric_string) if pa_id_col else ""
    out["F"] = df["F"].apply(normalize_tracking_mark) if "F" in df.columns else ""
    out["A"] = df["A"].apply(normalize_tracking_mark) if "A" in df.columns else ""
    out["U"] = df["U"].apply(normalize_tracking_mark) if "U" in df.columns else ""
    out["NH"] = df["NH"].apply(normalize_tracking_mark) if "NH" in df.columns else ""
    out["Yard Sign"] = df["Yard Sign"].apply(normalize_tracking_mark) if "Yard Sign" in df.columns else ""
    out["Notes"] = df["Notes"].apply(normalize_export_text) if "Notes" in df.columns else ""
    out["Contacted"] = df["Contacted"].apply(normalize_tracking_mark) if "Contacted" in df.columns else ""
    out["Result"] = df["Result"].apply(normalize_walk_result_value) if "Result" in df.columns else ""
    out["Support Level"] = df["Support Level"].apply(normalize_export_text) if "Support Level" in df.columns else ""
    out["Follow-Up"] = df["Follow-Up"].apply(normalize_tracking_mark) if "Follow-Up" in df.columns else ""
    out["Walk Notes"] = df["Walk Notes"].apply(normalize_export_text) if "Walk Notes" in df.columns else ""
    out["MB_Perm"] = df[mb_perm_col].apply(normalize_mb_perm_value) if mb_perm_col else ""
    out["HouseNumSort"] = house_vals.apply(parse_house_number)
    out["AptSort"] = apt_vals.apply(parse_apartment_sort)

    out = out.sort_values(by=["Precinct", "StreetGroup", "HouseNumSort", "AptSort", "FullName"], kind="stable").reset_index(drop=True)
    return out

def build_precinct_summary(street_df: pd.DataFrame) -> pd.DataFrame:
    if street_df.empty:
        return pd.DataFrame(columns=["Precinct","Individuals","Households"])
    temp = street_df.copy()
    temp["_hh"] = temp["Precinct"].astype(str) + "|" + temp["AddressLine"].astype(str)
    grp = temp.groupby("Precinct", dropna=False).agg(
        Individuals=("FullName","count"),
        Households=("_hh", lambda s: s.nunique())
    ).reset_index()
    grp = grp.sort_values("Precinct").reset_index(drop=True)
    return grp



def get_mb_perm_display(row) -> str:
    try:
        for key in ["MB_Perm", "MB_PERM", "MB_Perm_Display", "_MBPerm"]:
            if key in row:
                val = str(row.get(key, "")).strip().upper()
                if val in {"TRUE", "T", "YES", "Y", "1"}:
                    return "Y"
                if val in {"FALSE", "F", "NO", "N", "0"}:
                    return "N"
                if val in {"Y", "N"}:
                    return val
    except Exception:
        return ""
    return ""

def make_precinct_bookmark_key(precinct: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9]+", "_", str(precinct)).strip("_")
    return f"precinct_{safe}" if safe else "precinct_unknown"


REPORT_NAVY = colors.HexColor("#7A1523")
REPORT_RED = colors.HexColor("#9F2032")
REPORT_LIGHT = colors.HexColor("#F9E8EA")
REPORT_GRID = colors.HexColor("#D7B7BC")
REPORT_STREET = colors.HexColor("#F2D7DB")

def truncate_text(value, max_len):
    s = normalize_export_text(value)
    return s if len(s) <= max_len else s[:max_len - 1] + "…"

def make_precinct_bookmark_key(precinct: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9]+", "_", str(precinct)).strip("_")
    return f"precinct_{safe}" if safe else "precinct_unknown"

def draw_footer(c, page_num, total_pages, printed_date):
    width, _ = c._pagesize
    c.setStrokeColor(REPORT_GRID)
    c.line(32, 28, width - 32, 28)
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 8)
    c.drawCentredString(width / 2, 16, f"{page_num} of {total_pages}")
    c.drawRightString(width - 36, 16, f"Updated: {printed_date}")


def draw_brand(c, y_top):
    width, _ = c._pagesize
    try:
        if CC_LOGO.exists():
            c.drawImage(ImageReader(str(CC_LOGO)), 30, y_top - 30, width=108, height=30, preserveAspectRatio=True, mask='auto')
    except Exception:
        pass
    try:
        if TSS_LOGO.exists():
            c.drawImage(ImageReader(str(TSS_LOGO)), width - 118, y_top - 28, width=78, height=24, preserveAspectRatio=True, mask='auto')
    except Exception:
        pass
    c.setFillColor(REPORT_NAVY)
    c.setFont("Helvetica-Bold", 8)
    c.drawRightString(width - 40, y_top - 6, "Powered By")

def _street_pdf_precinct_pages(street_df: pd.DataFrame):
    body_top = 480
    body_bottom = 42
    row_h = 14
    pages = 0
    for precinct, grp in street_df.groupby("Precinct", sort=False):
        current_street = None
        y = body_top - 10
        pages += 1
        for (street, address), addr_grp in grp.groupby(["StreetGroup", "AddressLine"], sort=False, dropna=False):
            need = len(addr_grp) + 1  # address row + voter rows
            if current_street != street:
                need += 1
            if y - (need * row_h) < body_bottom:
                pages += 1
                y = body_top - 10
                current_street = None
            if current_street != street:
                y -= row_h
                current_street = street
            y -= row_h  # address
            y -= row_h * len(addr_grp)
    return pages

def estimate_street_pdf_pages(summary_df: pd.DataFrame, street_df: pd.DataFrame):
    rows_per_summary_page = 26
    summary_pages = max(1, math.ceil(len(summary_df) / rows_per_summary_page)) if len(summary_df) else 1
    return 1 + summary_pages + _street_pdf_precinct_pages(street_df)


def _draw_cover_page(c, width, height, county_desc, party_desc, printed_date, totals_ind, totals_hh, filter_lines, page_num, total_pages):
    c.setFillColor(REPORT_NAVY)
    c.roundRect(34, height - 255, width - 68, 110, 14, fill=1, stroke=0)

    try:
        if CC_LOGO.exists():
            c.drawImage(ImageReader(str(CC_LOGO)), width/2 - 150, height - 105, width=300, height=84, preserveAspectRatio=True, mask='auto')
    except Exception:
        pass

    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 22)
    c.drawCentredString(width / 2, height - 173, "Voter Contact List")
    c.setFont("Helvetica", 11)
    c.drawCentredString(width / 2, height - 195, printed_date)
    c.setFont("Helvetica-Bold", 12)
    c.drawCentredString(width / 2, height - 214, f"Individuals: {totals_ind:,}   Households: {totals_hh:,}")

    c.setFillColor(REPORT_NAVY)
    c.setFont("Helvetica-Bold", 15)
    c.drawString(52, height - 305, "Selected Voters")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 11)
    y = height - 327
    for line in filter_lines[:14]:
        c.drawString(62, y, f"• {line}")
        y -= 17
        if y < 114:
            break

    try:
        c.setFillColor(REPORT_NAVY)
        c.setFont("Helvetica-Bold", 10)
        c.drawCentredString(width / 2, 84, "Powered By")
        if TSS_LOGO.exists():
            c.drawImage(ImageReader(str(TSS_LOGO)), width/2 - 48, 42, width=96, height=30, preserveAspectRatio=True, mask='auto')
    except Exception:
        pass

    draw_footer(c, page_num, total_pages, printed_date)


def _draw_summary_page(c, width, height, chunk, printed_date, page_num, total_pages):
    draw_brand(c, height - 18)
    c.setFillColor(REPORT_NAVY)
    c.setFont("Helvetica-Bold", 17)
    c.drawString(40, height - 72, "Precinct Counts Summary")

    table_x = 40
    table_y_top = height - 96
    table_w = width - 80
    row_h = 18
    precinct_w = table_w - 180

    c.setFillColor(REPORT_NAVY)
    c.rect(table_x, table_y_top - row_h, table_w, row_h, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(table_x + 8, table_y_top - 12, "Precinct")
    c.drawRightString(table_x + precinct_w + 80, table_y_top - 12, "Individuals")
    c.drawRightString(table_x + table_w - 10, table_y_top - 12, "Households")

    y = table_y_top - row_h
    for i, (_, row) in enumerate(chunk.iterrows()):
        y -= row_h
        fill = REPORT_LIGHT if i % 2 == 0 else colors.white
        if normalize_export_text(row["Precinct"]).upper() == "TOTAL":
            fill = REPORT_STREET
        c.setFillColor(fill)
        c.rect(table_x, y, table_w, row_h, fill=1, stroke=0)
        c.setStrokeColor(REPORT_GRID)
        c.rect(table_x, y, table_w, row_h, fill=0, stroke=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold" if normalize_export_text(row["Precinct"]).upper() == "TOTAL" else "Helvetica", 9)
        c.drawString(table_x + 8, y + 5, truncate_text(row["Precinct"], 42))
        c.drawRightString(table_x + precinct_w + 80, y + 5, f"{int(row['Individuals']):,}")
        c.drawRightString(table_x + table_w - 10, y + 5, f"{int(row['Households']):,}")

    draw_footer(c, page_num, total_pages, printed_date)


def _draw_precinct_page_header(c, width, height, precinct, page_in_precinct):
    draw_brand(c, height - 18)
    title = precinct if page_in_precinct == 1 else f"{precinct} (cont)"
    c.setFillColor(REPORT_NAVY)
    c.setFont("Helvetica-Bold", 17)
    c.drawString(40, height - 74, title)

    c.setFillColor(REPORT_NAVY)
    c.roundRect(38, height - 106, width - 76, 22, 6, fill=1, stroke=0)

    cols = {
        "Full Name": 96, "Phone": 300, "Party": 448, "Sex": 478, "Age": 505,
        "F": 536, "A": 554, "U": 572, "NH": 590, "Yard Sign": 616, "MB Perm": 686
    }
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 8)
    for label, x in cols.items():
        c.drawString(x, height - 97, label)
    return cols

def generate_street_list_pdf_bytes(active_filters):
    street_df = build_street_list_dataframe(active_filters)
    street_df = apply_uploaded_street_result_filters(street_df)
    if street_df.empty:
        return b""

    street_df = street_df.fillna("")
    summary_df = build_precinct_summary(street_df)
    county_desc = selected_area_desc(active_filters)
    parties = active_filters.get("party_pick", []) or []
    party_desc = ", ".join(expand_party_label(p) for p in parties) if parties else "Filtered Voters"
    printed_date = datetime.now().strftime("%m/%d/%Y")
    filter_lines = build_filter_summary_lines(active_filters)

    summary_total = pd.DataFrame([{"Precinct":"TOTAL","Individuals":int(summary_df["Individuals"].sum()) if len(summary_df) else 0,"Households":int(summary_df["Households"].sum()) if len(summary_df) else 0}])
    summary_df_with_total = pd.concat([summary_df, summary_total], ignore_index=True)
    total_pages = estimate_street_pdf_pages(summary_df_with_total, street_df)

    buffer = BytesIO()
    page_size = landscape(letter)
    c = canvas.Canvas(buffer, pagesize=page_size)
    width, height = page_size
    page_num = 1

    totals_hh = int(summary_df["Households"].sum()) if len(summary_df) else 0
    totals_ind = int(summary_df["Individuals"].sum()) if len(summary_df) else 0
    _draw_cover_page(c, width, height, county_desc, party_desc, printed_date, totals_ind, totals_hh, filter_lines, page_num, total_pages)
    c.showPage()
    page_num += 1

    rows_per_summary_page = 26
    if len(summary_df_with_total) == 0:
        _draw_summary_page(c, width, height, summary_df_with_total, printed_date, page_num, total_pages)
        c.showPage()
        page_num += 1
    else:
        for start in range(0, len(summary_df_with_total), rows_per_summary_page):
            chunk = summary_df_with_total.iloc[start:start + rows_per_summary_page]
            _draw_summary_page(c, width, height, chunk, printed_date, page_num, total_pages)
            c.showPage()
            page_num += 1

    body_top = height - 104
    body_bottom = 40
    row_h = 14

    for precinct, grp in street_df.groupby("Precinct", sort=False):
        grp = grp.sort_values(["StreetGroup", "HouseNumSort", "AptSort", "FullName"], kind="stable")
        page_in_precinct = 1
        current_street = None
        cols = _draw_precinct_page_header(c, width, height, precinct, page_in_precinct)
        bookmark_key = make_precinct_bookmark_key(precinct)
        c.bookmarkPage(bookmark_key)
        c.addOutlineEntry(str(precinct), bookmark_key, level=0, closed=False)
        y = body_top - 10

        for (street, address), addr_grp in grp.groupby(["StreetGroup", "AddressLine"], sort=False, dropna=False):
            addr_grp = addr_grp.reset_index(drop=True)
            need = len(addr_grp) + 1
            if current_street != street:
                need += 1

            if y - (need * row_h) < body_bottom:
                draw_footer(c, page_num, total_pages, printed_date)
                c.showPage()
                page_num += 1
                page_in_precinct += 1
                cols = _draw_precinct_page_header(c, width, height, precinct, page_in_precinct)
                y = body_top - 10
                current_street = None

            if current_street != street:
                c.setFillColor(REPORT_STREET)
                c.rect(40, y - 9, width - 80, 14, fill=1, stroke=0)
                c.setFillColor(REPORT_NAVY)
                c.setFont("Helvetica-Bold", 10)
                c.drawString(48, y - 5, truncate_text(street, 80))
                y -= row_h
                current_street = street

            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(58, y - 5, truncate_text(address, 18))
            y -= row_h

            c.setFont("Helvetica", 8.5)
            for row_idx, (_, row) in enumerate(addr_grp.iterrows()):
                fill = REPORT_LIGHT if row_idx % 2 == 0 else colors.white
                c.setFillColor(fill)
                c.rect(52, y - 8, width - 104, 12, fill=1, stroke=0)

                c.setFillColor(colors.black)
                c.drawString(cols["Full Name"], y - 5, truncate_text(row["FullName"], 34))
                c.drawString(cols["Phone"], y - 5, truncate_text(row["Phone"], 22))
                c.drawString(cols["Party"], y - 5, truncate_text(row["Party"], 2))
                c.drawString(cols["Sex"], y - 5, truncate_text(row["Sex"], 1))
                c.drawString(cols["Age"], y - 5, truncate_text(row["Age"], 3))

                for label in ["F", "A", "U", "NH", "Yard Sign"]:
                    c.rect(cols[label], y - 7, 8, 8, fill=0, stroke=1)
                    mark_val = normalize_export_text(row.get(label, ""))
                    if mark_val:
                        c.setFont("Helvetica-Bold", 7.5)
                        c.drawCentredString(cols[label] + 4, y - 5, "X")
                        c.setFont("Helvetica", 8.5)

                mb_val = truncate_text(get_mb_perm_display(row), 1)
                if mb_val:
                    c.drawCentredString(cols["MB Perm"] + 4, y - 5, mb_val)
                y -= row_h

        draw_footer(c, page_num, total_pages, printed_date)
        if page_num < total_pages:
            c.showPage()
            page_num += 1

    c.save()
    return buffer.getvalue()



def _make_walk_sheet_groups(active_filters):
    street_df = build_street_list_dataframe(active_filters).copy()
    street_df = apply_uploaded_walk_result_filters(street_df)
    if street_df.empty:
        return street_df, []

    groups = []
    for precinct, precinct_df in street_df.groupby("Precinct", sort=False):
        precinct_df = precinct_df.sort_values(["StreetGroup", "HouseNumSort", "AptSort", "FullName"], kind="stable")
        for (street, address), addr_grp in precinct_df.groupby(["StreetGroup", "AddressLine"], sort=False, dropna=False):
            addr_grp = addr_grp.reset_index(drop=True)
            groups.append({
                "precinct": normalize_export_text(precinct),
                "street": normalize_export_text(street),
                "address": normalize_export_text(address),
                "rows": addr_grp.to_dict("records"),
            })
    return street_df, groups


def _estimate_walk_sheet_pages(groups, page_size):
    _, height = page_size
    body_top = height - 132
    body_bottom = 44
    address_h = 20
    voter_h = 20

    pages = 1 if groups else 0
    y = body_top
    last_precinct = None

    for group in groups:
        need = address_h + (len(group["rows"]) * voter_h) + 8
        if last_precinct is not None and group["precinct"] != last_precinct:
            need += 12
        if y - need < body_bottom:
            pages += 1
            y = body_top
        y -= need
        last_precinct = group["precinct"]

    return max(pages, 1)


def _draw_walk_sheet_header(c, width, height, precinct, page_in_precinct, printed_date, filter_desc):
    draw_brand(c, height - 16)
    title = precinct if precinct else "Selected Precinct"
    if page_in_precinct > 1:
        title = f"{title} (cont. {page_in_precinct})"

    c.setFillColor(REPORT_NAVY)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(24, height - 58, f"Walk Sheet – {title}")

    c.setFillColor(colors.black)
    c.setFont("Helvetica", 9)
    subtitle = truncate_text(filter_desc, 145)
    c.drawString(24, height - 74, subtitle)

    c.setFillColor(REPORT_NAVY)
    c.setFont("Helvetica", 8)
    c.drawString(24, height - 88, "C = Contact   N = Not Home   F = Follow-up")

    c.setFont("Helvetica-Bold", 8)
    c.drawString(32, height - 102, "C")
    c.drawString(51, height - 102, "N")
    c.drawString(70, height - 102, "F")
    c.drawString(94, height - 102, "Voter")
    c.drawString(300, height - 102, "Details")
    c.drawString(510, height - 102, "Notes")
    c.setStrokeColor(REPORT_GRID)
    c.line(24, height - 108, width - 24, height - 108)


def generate_walk_sheet_pdf_bytes(active_filters):
    street_df, groups = _make_walk_sheet_groups(active_filters)
    if street_df.empty or not groups:
        return b""

    page_size = landscape(letter)
    width, height = page_size
    printed_date = datetime.now().strftime("%m/%d/%Y")
    county_desc = selected_area_desc(active_filters)
    filter_lines = build_filter_summary_lines(active_filters)
    filter_desc = county_desc
    if filter_lines:
        filter_desc += " | " + " | ".join(filter_lines[:3])

    total_pages = _estimate_walk_sheet_pages(groups, page_size)
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=page_size)

    page_num = 1
    page_in_precinct = 1
    current_precinct = groups[0]["precinct"]

    _draw_walk_sheet_header(c, width, height, current_precinct, page_in_precinct, printed_date, filter_desc)

    body_top = height - 132
    body_bottom = 44
    address_h = 20
    voter_h = 20
    y = body_top

    for idx, group in enumerate(groups):
        if group["precinct"] != current_precinct:
            current_precinct = group["precinct"]
            page_in_precinct = 1

        needed = address_h + (len(group["rows"]) * voter_h) + 8
        if y - needed < body_bottom:
            draw_footer(c, page_num, total_pages, printed_date)
            c.showPage()
            page_num += 1
            if idx > 0 and groups[idx - 1]["precinct"] == group["precinct"]:
                page_in_precinct += 1
            else:
                page_in_precinct = 1
            _draw_walk_sheet_header(c, width, height, current_precinct, page_in_precinct, printed_date, filter_desc)
            y = body_top

        c.setFillColor(REPORT_LIGHT)
        c.roundRect(24, y - 15, width - 48, 17, 6, fill=1, stroke=0)
        c.setFillColor(REPORT_NAVY)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(32, y - 10, truncate_text(f"{group['street']}  |  {group['address']}", 110))
        y -= address_h

        for row in group["rows"]:
            row_y = y
            checkbox_y = row_y - 11
            c.setStrokeColor(REPORT_GRID)
            checkbox_positions = {"C": 28, "N": 47, "F": 66}
            for x in checkbox_positions.values():
                c.rect(x, checkbox_y, 10, 10, fill=0, stroke=1)

            if normalize_export_text(row.get("Contacted", "")):
                c.setFont("Helvetica-Bold", 8)
                c.drawCentredString(checkbox_positions["C"] + 5, row_y - 3, "X")
            result_key = normalize_export_text(row.get("Result", "")).upper().replace(" ", "")
            if result_key in {"NOTHOME", "NH"}:
                c.setFont("Helvetica-Bold", 8)
                c.drawCentredString(checkbox_positions["N"] + 5, row_y - 3, "X")
            if normalize_export_text(row.get("Follow-Up", "")):
                c.setFont("Helvetica-Bold", 8)
                c.drawCentredString(checkbox_positions["F"] + 5, row_y - 3, "X")

            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(92, row_y - 6, truncate_text(row.get("FullName", ""), 32))

            detail = " / ".join(
                part for part in [
                    truncate_text(row.get("Phone", ""), 18),
                    truncate_text(row.get("Party", ""), 2),
                    truncate_text(row.get("Sex", ""), 1),
                    truncate_text(row.get("Age", ""), 3),
                    "MB " + truncate_text(get_mb_perm_display(row), 1) if truncate_text(get_mb_perm_display(row), 1) else "",
                ]
                if part
            )
            c.setFont("Helvetica", 9)
            c.drawString(300, row_y - 6, truncate_text(detail, 40))

            notes_y = row_y - 8
            c.setStrokeColor(REPORT_GRID)
            c.line(500, notes_y, width - 28, notes_y)
            y -= voter_h

        y -= 8

    draw_footer(c, page_num, total_pages, printed_date)
    c.save()
    return buffer.getvalue()



def build_street_list_dataframe_from_detail_df(df: pd.DataFrame):
    df = merge_uploaded_street_results_into_detail_df(df)
    df = merge_uploaded_walk_results_into_detail_df(df)
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "Precinct","StreetGroup","AddressLine","FullName","Phone","Party","Sex","Age","PA ID Number",
            "F","A","U","NH","Yard Sign","Notes","Contacted","Result","Support Level","Follow-Up","Walk Notes","MB_Perm","HouseNumSort","AptSort"
        ])

    precinct_col = first_existing_detail(df.columns.tolist(), ["Precinct"])
    street_col = first_existing_detail(df.columns.tolist(), ["Street Name"])
    house_col = first_existing_detail(df.columns.tolist(), ["House Number"])
    apt_col = first_existing_detail(df.columns.tolist(), ["Apartment Number"])
    sex_col = first_existing_detail(df.columns.tolist(), ["Gender", "Sex"])
    age_col = first_existing_detail(df.columns.tolist(), ["Age"])
    party_col = first_existing_detail(df.columns.tolist(), ["Party"])
    pa_id_col = first_existing_detail(df.columns.tolist(), ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"])
    mb_perm_col = first_existing_detail(df.columns.tolist(), ["MB_PERM", "MB_Perm", "MB_Pern"])

    out = pd.DataFrame()
    out["Precinct"] = df[precinct_col].apply(normalize_export_text) if precinct_col else ""
    out["StreetGroup"] = df[street_col].apply(normalize_address_value) if street_col else ""
    house_vals = df[house_col].apply(normalize_export_text) if house_col else pd.Series([""] * len(df))
    apt_vals = df[apt_col].apply(normalize_export_text) if apt_col else pd.Series([""] * len(df))
    out["AddressLine"] = house_vals
    out.loc[apt_vals != "", "AddressLine"] = out.loc[apt_vals != "", "AddressLine"] + " Apt " + apt_vals[apt_vals != ""]
    out["AddressLine"] = out["AddressLine"].apply(collapse_spaces).apply(normalize_address_value)
    out["FullName"] = df.apply(full_name_from_row, axis=1).apply(normalize_name_value)
    out["Phone"] = df.apply(choose_best_phone, axis=1)
    out["Party"] = df[party_col].apply(normalize_export_text) if party_col else ""
    out["Sex"] = df[sex_col].apply(normalize_export_text) if sex_col else ""
    out["Age"] = df[age_col].apply(lambda v: normalize_numeric_string(v)) if age_col else ""
    out["PA ID Number"] = df[pa_id_col].apply(normalize_numeric_string) if pa_id_col else ""
    out["F"] = df["F"].apply(normalize_tracking_mark) if "F" in df.columns else ""
    out["A"] = df["A"].apply(normalize_tracking_mark) if "A" in df.columns else ""
    out["U"] = df["U"].apply(normalize_tracking_mark) if "U" in df.columns else ""
    out["NH"] = df["NH"].apply(normalize_tracking_mark) if "NH" in df.columns else ""
    out["Yard Sign"] = df["Yard Sign"].apply(normalize_tracking_mark) if "Yard Sign" in df.columns else ""
    out["Notes"] = df["Notes"].apply(normalize_export_text) if "Notes" in df.columns else ""
    out["Contacted"] = df["Contacted"].apply(normalize_tracking_mark) if "Contacted" in df.columns else ""
    out["Result"] = df["Result"].apply(normalize_walk_result_value) if "Result" in df.columns else ""
    out["Support Level"] = df["Support Level"].apply(normalize_export_text) if "Support Level" in df.columns else ""
    out["Follow-Up"] = df["Follow-Up"].apply(normalize_tracking_mark) if "Follow-Up" in df.columns else ""
    out["Walk Notes"] = df["Walk Notes"].apply(normalize_export_text) if "Walk Notes" in df.columns else ""
    out["MB_Perm"] = df[mb_perm_col].apply(normalize_mb_perm_value) if mb_perm_col else ""
    out["HouseNumSort"] = house_vals.apply(parse_house_number)
    out["AptSort"] = apt_vals.apply(parse_apartment_sort)

    out = out.sort_values(by=["Precinct", "StreetGroup", "HouseNumSort", "AptSort", "FullName"], kind="stable").reset_index(drop=True)
    return out

def make_walk_sheet_groups_from_street_df(street_df: pd.DataFrame):
    if street_df is None or street_df.empty:
        return []

    groups = []
    for precinct, precinct_df in street_df.groupby("Precinct", sort=False):
        precinct_df = precinct_df.sort_values(["StreetGroup", "HouseNumSort", "AptSort", "FullName"], kind="stable")
        for (street, address), addr_grp in precinct_df.groupby(["StreetGroup", "AddressLine"], sort=False, dropna=False):
            addr_grp = addr_grp.reset_index(drop=True)
            groups.append({
                "precinct": normalize_export_text(precinct),
                "street": normalize_export_text(street),
                "address": normalize_export_text(address),
                "rows": addr_grp.to_dict("records"),
            })
    return groups

def generate_walk_sheet_pdf_from_street_df(street_df: pd.DataFrame, title: str, filter_desc: str = ""):
    if street_df is None or street_df.empty:
        return b""

    groups = make_walk_sheet_groups_from_street_df(street_df)
    if not groups:
        return b""

    page_size = landscape(letter)
    width, height = page_size
    printed_date = datetime.now().strftime("%m/%d/%Y")
    total_pages = _estimate_walk_sheet_pages(groups, page_size)
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=page_size)

    page_num = 1
    page_in_precinct = 1
    current_precinct = groups[0]["precinct"]

    header_title = title or current_precinct or "Selected Turf"
    header_desc = filter_desc or "Turf packet walk sheet"
    _draw_walk_sheet_header(c, width, height, header_title, page_in_precinct, printed_date, header_desc)

    body_top = height - 132
    body_bottom = 44
    address_h = 20
    voter_h = 20
    y = body_top

    for idx, group in enumerate(groups):
        if group["precinct"] != current_precinct:
            current_precinct = group["precinct"]
            page_in_precinct = 1

        needed = address_h + (len(group["rows"]) * voter_h) + 8
        if y - needed < body_bottom:
            draw_footer(c, page_num, total_pages, printed_date)
            c.showPage()
            page_num += 1
            if idx > 0 and groups[idx - 1]["precinct"] == group["precinct"]:
                page_in_precinct += 1
            else:
                page_in_precinct = 1
            _draw_walk_sheet_header(c, width, height, header_title, page_in_precinct, printed_date, header_desc)
            y = body_top

        c.setFillColor(REPORT_LIGHT)
        c.roundRect(24, y - 15, width - 48, 17, 6, fill=1, stroke=0)
        c.setFillColor(REPORT_NAVY)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(32, y - 10, truncate_text(f"{group['street']}  |  {group['address']}", 110))
        y -= address_h

        for row in group["rows"]:
            row_y = y
            checkbox_y = row_y - 11
            c.setStrokeColor(REPORT_GRID)
            for x in (28, 47, 66):
                c.rect(x, checkbox_y, 10, 10, fill=0, stroke=1)

            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(92, row_y - 6, truncate_text(row.get("FullName", ""), 32))

            detail = " / ".join(
                part for part in [
                    truncate_text(row.get("Phone", ""), 18),
                    truncate_text(row.get("Party", ""), 2),
                    truncate_text(row.get("Sex", ""), 1),
                    truncate_text(row.get("Age", ""), 3),
                    "MB " + truncate_text(get_mb_perm_display(row), 1) if truncate_text(get_mb_perm_display(row), 1) else "",
                ]
                if part
            )
            c.setFont("Helvetica", 9)
            c.drawString(300, row_y - 6, truncate_text(detail, 40))

            notes_y = row_y - 8
            c.setStrokeColor(REPORT_GRID)
            c.line(500, notes_y, width - 28, notes_y)
            y -= voter_h

        y -= 8

    draw_footer(c, page_num, total_pages, printed_date)
    c.save()
    return buffer.getvalue()

def _summary_count_df(active_filters, columns, group_expr, label_alias="Label", include_blank=True):
    con = get_conn()
    where_sql, params = current_filter_clause(active_filters, columns)
    blank_filter = "" if include_blank else f" AND {group_expr} IS NOT NULL AND trim(cast({group_expr} as varchar)) <> ''"
    return con.execute(
        f"""
        SELECT
            coalesce(nullif(trim(cast({group_expr} as varchar)), ''), 'Blank/Unknown') AS {quote_ident(label_alias)},
            count(*) AS Count
        FROM voters
        {where_sql}
        {blank_filter}
        GROUP BY 1
        ORDER BY Count DESC, 1
        """,
        params,
    ).df()


def _summary_age_df(active_filters, columns):
    con = get_conn()
    where_sql, params = current_filter_clause(active_filters, columns)
    return con.execute(
        f"""
        SELECT
            case
                when _AgeNum IS NULL then 'Blank/Unknown'
                when _AgeNum < 18 then 'Under 18'
                when _AgeNum <= 24 then '18-24'
                when _AgeNum <= 34 then '25-34'
                when _AgeNum <= 44 then '35-44'
                when _AgeNum <= 54 then '45-54'
                when _AgeNum <= 64 then '55-64'
                when _AgeNum <= 74 then '65-74'
                else '75+'
            end AS AgeBucket,
            count(*) AS Count,
            case
                when _AgeNum IS NULL then 99
                when _AgeNum < 18 then 1
                when _AgeNum <= 24 then 2
                when _AgeNum <= 34 then 3
                when _AgeNum <= 44 then 4
                when _AgeNum <= 54 then 5
                when _AgeNum <= 64 then 6
                when _AgeNum <= 74 then 7
                else 8
            end AS SortKey
        FROM voters
        {where_sql}
        GROUP BY 1, 3
        ORDER BY SortKey
        """,
        params,
    ).df()[["AgeBucket", "Count"]]


def generate_summary_report_pdf_bytes(active_filters, columns):
    metrics = query_metrics(active_filters, columns)
    party_df = _summary_count_df(active_filters, columns, "_PartyNorm", "Value")
    gender_df = _summary_count_df(active_filters, columns, "_Gender", "Value")
    age_df = _summary_age_df(active_filters, columns)
    filter_lines = build_filter_summary_lines(active_filters)
    printed_dt = datetime.now().strftime("%m/%d/%Y %I:%M %p")

    buffer = BytesIO()
    page_size = landscape(letter)
    width, height = page_size
    c = canvas.Canvas(buffer, pagesize=page_size)

    def section_bar(y, title):
        c.setFillColor(REPORT_NAVY)
        c.roundRect(26, y - 14, width - 52, 18, 6, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(34, y - 9, title)

    def draw_simple_table(x, y_top, headers, rows, col_widths, row_h=16, font_size=8):
        table_w = sum(col_widths)
        c.setFillColor(REPORT_NAVY)
        c.rect(x, y_top - row_h, table_w, row_h, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", font_size)
        cursor = x
        for idx, head in enumerate(headers):
            if idx == len(headers) - 1:
                c.drawRightString(cursor + col_widths[idx] - 6, y_top - 11, str(head))
            else:
                c.drawString(cursor + 6, y_top - 11, str(head))
            cursor += col_widths[idx]

        y = y_top - row_h
        for i, row in enumerate(rows):
            y -= row_h
            fill = REPORT_LIGHT if i % 2 == 0 else colors.white
            c.setFillColor(fill)
            c.rect(x, y, table_w, row_h, fill=1, stroke=0)
            c.setStrokeColor(REPORT_GRID)
            c.rect(x, y, table_w, row_h, fill=0, stroke=1)
            c.setFillColor(colors.black)
            c.setFont("Helvetica", font_size)
            cursor = x
            for idx, cell in enumerate(row):
                cell_text = truncate_text(cell, 48)
                if idx == len(row) - 1:
                    c.drawRightString(cursor + col_widths[idx] - 6, y + 4, cell_text)
                else:
                    c.drawString(cursor + 6, y + 4, cell_text)
                cursor += col_widths[idx]
        return y

    draw_brand(c, height - 18)
    c.setFillColor(REPORT_NAVY)
    c.setFont("Helvetica-Bold", 20)
    c.drawString(28, height - 58, "Candidate Connect Summary Report")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 10)
    c.drawString(28, height - 74, f"Generated: {printed_dt}")

    section_bar(height - 96, "Overview")
    overview_rows = [
        ["Total Voters", f"{int(metrics.get('voters', 0)):,}"],
        ["Total Households", f"{int(metrics.get('households', 0)):,}"],
        ["With Email", f"{int(metrics.get('emails', 0)):,}"],
        ["With Landline", f"{int(metrics.get('landlines', 0)):,}"],
        ["With Mobile", f"{int(metrics.get('mobiles', 0)):,}"],
    ]
    draw_simple_table(28, height - 104, ["Metric", "Value"], overview_rows, [180, 90])

    section_bar(height - 228, "Selected Filters")
    if not filter_lines:
        filter_lines = ["No additional filters selected"]
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 9)
    fy = height - 250
    for line in filter_lines[:10]:
        c.drawString(34, fy, u"• " + truncate_text(line, 135))
        fy -= 14

    left_x = 28
    right_x = 405
    top_y = height - 410

    section_bar(top_y, "Party Breakdown")
    party_rows = [[str(r["Value"]), f"{int(r['Count']):,}"] for _, r in party_df.iterrows()] or [["No data", "0"]]
    y_end_left = draw_simple_table(left_x, top_y - 8, ["Value", "Count"], party_rows[:10], [180, 90])

    section_bar(top_y, "Gender Breakdown")
    gender_rows = [[str(r["Value"]), f"{int(r['Count']):,}"] for _, r in gender_df.iterrows()] or [["No data", "0"]]
    y_end_right = draw_simple_table(right_x, top_y - 8, ["Value", "Count"], gender_rows[:10], [180, 90])

    lower_top = min(y_end_left, y_end_right) - 26
    section_bar(lower_top, "Age Breakdown")
    age_rows = [[str(r["AgeBucket"]), f"{int(r['Count']):,}"] for _, r in age_df.iterrows()] or [["No data", "0"]]
    draw_simple_table(28, lower_top - 8, ["Age Range", "Count"], age_rows[:10], [180, 90])

    draw_footer(c, 1, 1, datetime.now().strftime("%m/%d/%Y"))
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



def format_lookup_date(value) -> str:
    if value is None:
        return ""
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return normalize_export_text(value)
        return ts.strftime("%m/%d/%Y")
    except Exception:
        return normalize_export_text(value)


def format_lookup_phone(value) -> str:
    digits = clean_phone_value(value)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return normalize_export_text(value)


def get_detail_columns(detail_paths):
    con = get_conn()
    paths_sql = "[" + ", ".join(sql_string_literal(p) for p in detail_paths) + "]"
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet({paths_sql})").df()
    return df["column_name"].tolist()


def _detail_col_expr(columns, candidates, fallback="''"):
    col = first_existing_detail(columns, candidates)
    if col is None:
        return fallback, None
    return f"coalesce(cast(src.{quote_ident(col)} as varchar), '')", col


@st.cache_data(show_spinner=False)
def get_detail_distinct_values(detail_paths, column_name: str):
    paths_sql = "[" + ", ".join(sql_string_literal(p) for p in detail_paths) + "]"
    qcol = quote_ident(column_name)
    df = get_conn().execute(
        f'''
        SELECT DISTINCT trim(cast({qcol} as varchar)) AS value
        FROM read_parquet({paths_sql}, union_by_name=True)
        WHERE nullif(trim(cast({qcol} as varchar)), '') IS NOT NULL
        ORDER BY 1
        '''
    ).df()
    return [normalize_export_text(v) for v in df["value"].tolist() if normalize_export_text(v) != ""]


def _normalize_lookup_place(value: str) -> str:
    s = normalize_export_text(value).upper()
    s = re.sub(r"\bCOUNTY\b", "", s)
    s = re.sub(r"\bCO\b", "", s)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_lookup_search(search_text: str, detail_paths, detail_columns):
    raw = normalize_export_text(search_text)
    parsed = {
        "raw": raw,
        "email": "",
        "phone_digits": "",
        "zip5": "",
        "pa_id_digits": "",
        "county": "",
        "remaining_tokens": [],
    }
    if not raw:
        return parsed

    email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", raw)
    if email_match:
        parsed["email"] = email_match.group(0).strip()

    phone_match = re.search(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", raw)
    if phone_match:
        parsed["phone_digits"] = "".join(re.findall(r"\d", phone_match.group(0)))[-10:]

    zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", raw)
    if zip_match:
        parsed["zip5"] = zip_match.group(1)

    pa_id_match = re.search(r"\b\d{6,}(?:-\d+)?\b", raw)
    if pa_id_match and not parsed["phone_digits"]:
        parsed["pa_id_digits"] = "".join(re.findall(r"\d", pa_id_match.group(0)))

    county_map = {}
    if "County" in detail_columns:
        for county in get_detail_distinct_values(detail_paths, "County"):
            norm = _normalize_lookup_place(county)
            if norm:
                county_map[norm] = county

    normalized_query = _normalize_lookup_place(raw)
    for county_norm, county_label in county_map.items():
        if county_norm and re.search(rf"(^| )({re.escape(county_norm)})( |$)", normalized_query):
            parsed["county"] = county_label
            normalized_query = re.sub(rf"(^| )({re.escape(county_norm)})( |$)", " ", normalized_query).strip()
            break

    cleaned = raw
    if parsed["email"]:
        cleaned = cleaned.replace(parsed["email"], " ")
    if parsed["phone_digits"]:
        cleaned = re.sub(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", " ", cleaned)
    if parsed["zip5"]:
        cleaned = re.sub(rf"\b{re.escape(parsed['zip5'])}(?:-\d{{4}})?\b", " ", cleaned)
    if parsed["pa_id_digits"]:
        cleaned = re.sub(r"[0-9-]{6,}", " ", cleaned)
    if parsed["county"]:
        cleaned = re.sub(re.escape(parsed["county"]), " ", cleaned, flags=re.I)

    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", cleaned)
    parsed["remaining_tokens"] = [tok.upper() for tok in cleaned.split() if len(tok.strip()) >= 2]
    return parsed


def search_voters_for_lookup(active_filters, search_text: str, limit: int = 50, use_current_filters: bool = False) -> pd.DataFrame:
    detail_paths, _ = ensure_detail_shards()
    detail_columns = get_detail_columns(detail_paths)
    lookup_filters = active_filters if use_current_filters else {}
    base_sql, base_params = build_detail_export_sql(detail_paths, lookup_filters)

    first_expr, first_col = _detail_col_expr(detail_columns, ["FirstName", "First Name"])
    middle_expr, middle_col = _detail_col_expr(detail_columns, ["MiddleName", "Middle Name"])
    last_expr, last_col = _detail_col_expr(detail_columns, ["LastName", "Last Name"])
    suffix_expr, suffix_col = _detail_col_expr(detail_columns, ["NameSuffix", "Suffix", "Name Suffix"])
    full_name_expr, full_name_col = _detail_col_expr(detail_columns, ["FullName", "Full Name", "Name"], fallback=None)
    house_expr, house_col = _detail_col_expr(detail_columns, ["House Number", "HouseNumber", "Street Number"])
    street_expr, street_col = _detail_col_expr(detail_columns, ["Street Name", "StreetName", "Street"])
    apt_expr, apt_col = _detail_col_expr(detail_columns, ["Apartment Number", "ApartmentNumber", "Unit", "Apt"])
    city_expr, city_col = _detail_col_expr(detail_columns, ["MailingCity", "Mailing City", "City", "MailCity"])
    state_expr, state_col = _detail_col_expr(detail_columns, ["MailingState", "Mailing State", "State", "MailState"])
    zip_expr, zip_col = _detail_col_expr(detail_columns, ["MailingZip", "Mailing Zip", "ZIP", "Zip", "ZipCode", "ZIPCODE", "MailZip"])
    email_expr, email_col = _detail_col_expr(detail_columns, ["Email", "EmailAddress", "Email Address"])
    mobile_expr, mobile_col = _detail_col_expr(detail_columns, ["Mobile", "Cell", "CellPhone", "Cell Phone"])
    landline_expr, landline_col = _detail_col_expr(detail_columns, ["Landline", "Phone", "HomePhone", "PrimaryPhone", "Primary Phone"])
    pa_id_expr, pa_id_col = _detail_col_expr(detail_columns, ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"])
    county_expr = "coalesce(cast(src.\"County\" as varchar), '')" if "County" in detail_columns else "''"
    muni_expr = "coalesce(cast(src.\"Municipality\" as varchar), '')" if "Municipality" in detail_columns else "''"
    precinct_expr = "coalesce(cast(src.\"Precinct\" as varchar), '')" if "Precinct" in detail_columns else "''"

    if full_name_col:
        lookup_name_expr = full_name_expr
    else:
        lookup_name_expr = f"trim(concat_ws(' ', {first_expr}, {middle_expr}, {last_expr}, {suffix_expr}))"

    lookup_address_expr = f"trim(concat_ws(' ', {house_expr}, {street_expr}, case when trim({apt_expr}) <> '' then concat('Apt ', trim({apt_expr})) else '' end))"
    lookup_city_state_zip_expr = f"trim(concat_ws(', ', nullif(trim({city_expr}), ''), trim(concat_ws(' ', nullif(trim({state_expr}), ''), nullif(trim({zip_expr}), '')))))"
    lookup_key_expr = f"trim(concat_ws('|', nullif(trim({pa_id_expr}), ''), {lookup_name_expr}, {lookup_address_expr}))"

    name_haystack_expr = f"upper(concat_ws(' ', {lookup_name_expr}, {first_expr}, {middle_expr}, {last_expr}, {suffix_expr}))"
    address_haystack_expr = f"upper(concat_ws(' ', {lookup_address_expr}, {city_expr}, {state_expr}, {zip_expr}, {county_expr}, {muni_expr}, {precinct_expr}))"
    general_haystack_expr = f"upper(concat_ws(' ', {name_haystack_expr}, {address_haystack_expr}, {pa_id_expr}, {email_expr}, {mobile_expr}, {landline_expr}))"

    parsed = parse_lookup_search(search_text, detail_paths, detail_columns)
    params = list(base_params)
    where_parts = []

    if parsed["county"] and "County" in detail_columns:
        where_parts.append(f"upper(trim({county_expr})) = ?")
        params.append(parsed["county"].upper())

    if parsed["email"]:
        where_parts.append(f"upper(trim({email_expr})) = ?")
        params.append(parsed["email"].upper())

    if parsed["phone_digits"]:
        cleaned_mobile_expr = f"regexp_replace({mobile_expr}, '[^0-9]', '', 'g')"
        cleaned_landline_expr = f"regexp_replace({landline_expr}, '[^0-9]', '', 'g')"
        where_parts.append(f"({cleaned_mobile_expr} LIKE ? OR {cleaned_landline_expr} LIKE ?)")
        params.extend([f"%{parsed['phone_digits']}%", f"%{parsed['phone_digits']}%"])

    if parsed["zip5"]:
        cleaned_zip_expr = f"regexp_replace({zip_expr}, '[^0-9]', '', 'g')"
        where_parts.append(f"{cleaned_zip_expr} LIKE ?")
        params.append(f"{parsed['zip5']}%")

    if parsed["pa_id_digits"]:
        cleaned_paid_expr = f"regexp_replace({pa_id_expr}, '[^0-9]', '', 'g')"
        where_parts.append(f"{cleaned_paid_expr} = ?")
        params.append(parsed["pa_id_digits"])

    remaining_tokens = parsed["remaining_tokens"][:6]
    for tok in remaining_tokens:
        if tok.isdigit():
            where_parts.append(f"{general_haystack_expr} LIKE ?")
            params.append(f"%{tok}%")
        else:
            where_parts.append(f"({name_haystack_expr} LIKE ? OR {address_haystack_expr} LIKE ?)")
            params.extend([f"%{tok}%", f"%{tok}%"])

    if not where_parts:
        return pd.DataFrame()

    order_sql = "_LookupName, _LookupAddress"
    if remaining_tokens:
        exact_name = " ".join(remaining_tokens).upper()
        order_sql = f"case when upper(trim({lookup_name_expr})) = ? then 0 else 1 end, _LookupName, _LookupAddress"
        params.append(exact_name)

    sql = f'''
        SELECT
            src.*,
            {lookup_name_expr} AS _LookupName,
            {lookup_address_expr} AS _LookupAddress,
            {lookup_city_state_zip_expr} AS _LookupCityStateZip,
            {pa_id_expr} AS _LookupPAID,
            {lookup_key_expr} AS _LookupRowKey
        FROM ({base_sql}) src
        WHERE 1=1
        AND {' AND '.join(where_parts)}
        ORDER BY {order_sql}
        LIMIT {int(limit)}
    '''
    return get_conn().execute(sql, params).df()

def get_lookup_selected_row(results_df: pd.DataFrame):
    if results_df is None or results_df.empty:
        return None
    selected_key = st.session_state.get("lookup_selected_key", "")
    if selected_key:
        hit = results_df[results_df["_LookupRowKey"].astype(str) == str(selected_key)]
        if not hit.empty:
            return hit.iloc[0]
    return results_df.iloc[0]


def get_lookup_value(row, candidates, formatter=None) -> str:
    for col in candidates:
        if col in row.index:
            value = row.get(col)
            if formatter is not None:
                rendered = formatter(value)
            else:
                rendered = normalize_export_text(value)
            if normalize_export_text(rendered) != "":
                return rendered
    return ""


def build_lookup_full_name(row) -> str:
    full_name = get_lookup_value(row, ["FullName", "Full Name", "Name"])
    if full_name:
        return normalize_name_value(full_name)
    parts = [
        get_lookup_value(row, ["FirstName", "First Name"]),
        get_lookup_value(row, ["MiddleName", "Middle Name"]),
        get_lookup_value(row, ["LastName", "Last Name"]),
        get_lookup_value(row, ["NameSuffix", "Suffix", "Name Suffix"]),
    ]
    return normalize_name_value(" ".join([p for p in parts if p]).strip())


def build_lookup_address(row) -> str:
    line1 = normalize_address_value(" ".join([
        get_lookup_value(row, ["House Number", "HouseNumber", "Street Number"]),
        get_lookup_value(row, ["Street Name", "StreetName", "Street"]),
    ]).strip())
    apt = get_lookup_value(row, ["Apartment Number", "ApartmentNumber", "Unit", "Apt"])
    if apt:
        line1 = f"{line1} Apt {apt}".strip()
    city = normalize_city_value(get_lookup_value(row, ["MailingCity", "Mailing City", "City", "MailCity"]))
    state = normalize_state_value(get_lookup_value(row, ["MailingState", "Mailing State", "State", "MailState"]))
    zip_code = clean_zip_value(get_lookup_value(row, ["MailingZip", "Mailing Zip", "ZIP", "Zip", "ZipCode", "ZIPCODE", "MailZip"]))
    line2 = " ".join([p for p in [city + "," if city else "", state, zip_code] if p]).strip().replace(" ,", ",")
    if line1 and line2:
        return f"{line1}\n{line2}"
    return line1 or line2


def render_lookup_field_block(title: str, rows: list[tuple[str, str]]):
    clean_rows = [{"Field": label, "Value": value} for label, value in rows if normalize_export_text(value) != ""]
    st.markdown(f"#### {title}")
    if not clean_rows:
        st.caption("No data available")
    else:
        st.dataframe(pd.DataFrame(clean_rows), use_container_width=True, hide_index=True)


def render_voter_lookup_tab(active_filters, columns):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-header">Voter Lookup</div>', unsafe_allow_html=True)
    st.caption("Search the full statewide active voter file by name, county, address, PA ID, phone, or email.")

    search_cols = st.columns([2.3, 0.9, 0.9], gap="medium")
    with search_cols[0]:
        lookup_query = st.text_input(
            "Search voters",
            value=st.session_state.get("lookup_query", ""),
            placeholder="Example: Jane Smith Lancaster, Jane Smith 17520, PA ID, phone, or email",
            key="lookup_query_input",
        )
    with search_cols[1]:
        result_limit = st.selectbox("Max Results", [10, 25, 50, 100], index=1, key="lookup_result_limit")
    with search_cols[2]:
        st.markdown("")
        search_clicked = st.button("Search", use_container_width=True, type="primary", key="lookup_search_button")

    if st.button("Clear Lookup", use_container_width=False, key="lookup_clear_button"):
        st.session_state["lookup_query"] = ""
        st.session_state["lookup_query_input"] = ""
        st.session_state["lookup_results_records"] = []
        st.session_state["lookup_selected_key"] = ""
        st.rerun()

    should_search = search_clicked or (
        lookup_query
        and lookup_query != st.session_state.get("lookup_last_query", "")
        and len(lookup_query.strip()) >= 2
    )

    if should_search:
        with st.spinner("Searching voter detail shards..."):
            results_df = search_voters_for_lookup(active_filters, lookup_query.strip(), limit=int(result_limit), use_current_filters=False)
            st.session_state["lookup_query"] = lookup_query.strip()
            st.session_state["lookup_last_query"] = lookup_query.strip()
            st.session_state["lookup_results_records"] = results_df.to_dict("records")
            st.session_state["lookup_selected_key"] = results_df.iloc[0]["_LookupRowKey"] if not results_df.empty else ""

    results_df = pd.DataFrame(st.session_state.get("lookup_results_records", []))

    if not lookup_query.strip():
        st.info("Type a name, address, PA ID, phone number, or email to find a voter.")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    if results_df.empty:
        st.warning("No voters matched that search in the statewide active voter file.")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    st.caption(f"{len(results_df):,} result(s) loaded")
    left_col, right_col = st.columns([1.1, 1.9], gap="medium")

    with left_col:
        st.markdown("#### Results")
        for _, result_row in results_df.iterrows():
            title = normalize_name_value(normalize_export_text(result_row.get("_LookupName", ""))) or "Unnamed voter"
            line1 = normalize_address_value(normalize_export_text(result_row.get("_LookupAddress", "")))
            line2 = normalize_export_text(result_row.get("_LookupCityStateZip", ""))
            paid = normalize_numeric_string(result_row.get("_LookupPAID", ""))
            label_parts = [title]
            if line1:
                label_parts.append(line1)
            if line2:
                label_parts.append(line2)
            if paid:
                label_parts.append(f"PA ID {paid}")
            button_label = "\n".join(label_parts)
            if st.button(button_label, key=f"lookup_pick_{result_row.get('_LookupRowKey', '')}", use_container_width=True):
                st.session_state["lookup_selected_key"] = result_row.get("_LookupRowKey", "")
                st.rerun()

    selected_row = get_lookup_selected_row(results_df)
    if selected_row is None:
        st.markdown('</div>', unsafe_allow_html=True)
        return

    with right_col:
        voter_name = build_lookup_full_name(selected_row) or normalize_name_value(normalize_export_text(selected_row.get("_LookupName", ""))) or "Voter profile"
        st.markdown(f"### {voter_name}")
        address_block = build_lookup_address(selected_row)
        if address_block:
            st.markdown(address_block.replace("\n", "  \n"))

        top_metrics = st.columns(4, gap="small")
        top_metrics[0].metric("Party", get_lookup_value(selected_row, ["Party", "_PartyNorm"]) or "—")
        top_metrics[1].metric("Gender", get_lookup_value(selected_row, ["Gender", "Sex", "_Gender"]) or "—")
        top_metrics[2].metric("Age", get_lookup_value(selected_row, ["Age"], formatter=lambda v: normalize_numeric_string(v)) or "—")
        top_metrics[3].metric("PA ID", get_lookup_value(selected_row, ["PA ID Number", "PA_ID_Number", "PA ID", "StateVoterID", "State Voter ID", "Voter ID", "VoterID"], formatter=lambda v: normalize_numeric_string(v)) or "—")

        info_cols = st.columns(2, gap="medium")
        with info_cols[0]:
            render_lookup_field_block("Voter Details", [
                ("Date of Birth", get_lookup_value(selected_row, ["DOB", "Date of Birth", "BirthDate", "Birth Date"], formatter=format_lookup_date)),
                ("Registration Date", get_lookup_value(selected_row, ["RegistrationDate", "Registration Date", "_RegistrationDate"], formatter=format_lookup_date)),
                ("County", get_lookup_value(selected_row, ["County"])),
                ("Municipality", get_lookup_value(selected_row, ["Municipality"])),
                ("Precinct", get_lookup_value(selected_row, ["Precinct"])),
                ("Congressional", get_lookup_value(selected_row, ["USC", "Congressional District", "Congress District"])),
                ("State Senate", get_lookup_value(selected_row, ["STS", "State Senate", "Senate District"])),
                ("State House", get_lookup_value(selected_row, ["STH", "State House", "House District"])),
                ("School District", get_lookup_value(selected_row, ["School District", "SchoolDistrict"])),
            ])
        with info_cols[1]:
            render_lookup_field_block("Contact + Mail Ballot", [
                ("Mobile", get_lookup_value(selected_row, ["Mobile", "Cell", "CellPhone", "Cell Phone"], formatter=format_lookup_phone)),
                ("Landline", get_lookup_value(selected_row, ["Landline", "Phone", "HomePhone", "PrimaryPhone", "Primary Phone"], formatter=format_lookup_phone)),
                ("Email", get_lookup_value(selected_row, ["Email", "EmailAddress", "Email Address"])),
                ("Mail Ballot Applied", get_lookup_value(selected_row, ["MIB_Applied", "_MIBApplied"])),
                ("Mail Ballot Status", get_lookup_value(selected_row, ["MIB_BALLOT", "_MIBBallot"])),
                ("Permanent Mail", get_lookup_value(selected_row, ["MB_PERM", "MB_Perm", "MB_Pern", "_MBPerm"])),
                ("Mail Ballot Score", get_lookup_value(selected_row, ["MB_AProp_Score", "MMB_AProp_Score", "_MBScore"])),
            ])

        st.markdown("#### Vote History Summary")
        vote_cols = st.columns(3, gap="small")
        vote_cols[0].metric("All Elections (V4A)", get_lookup_value(selected_row, ["V4A"], formatter=lambda v: normalize_numeric_string(v)) or "—")
        vote_cols[1].metric("General (V4G)", get_lookup_value(selected_row, ["V4G"], formatter=lambda v: normalize_numeric_string(v)) or "—")
        vote_cols[2].metric("Primary (V4P)", get_lookup_value(selected_row, ["V4P"], formatter=lambda v: normalize_numeric_string(v)) or "—")

    st.markdown('</div>', unsafe_allow_html=True)

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
if "saved_universes" not in st.session_state:
    st.session_state.saved_universes = load_saved_universes()
if "street_results_df" not in st.session_state:
    st.session_state.street_results_df = pd.DataFrame(columns=["PA ID Number", "F", "A", "U", "NH", "Yard Sign", "Notes"])
if "street_results_filters" not in st.session_state:
    st.session_state.street_results_filters = {}
if "walk_results_df" not in st.session_state:
    st.session_state.walk_results_df = pd.DataFrame(columns=["PA ID Number", "Contacted", "Result", "Support Level", "Follow-Up", "Notes"])
if "walk_results_filters" not in st.session_state:
    st.session_state.walk_results_filters = {}

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
                vh_type_options = ["All", "General", "Primary"]
                current_vh_type = st.session_state.active_filters.get("vote_history_type", "All")
                if current_vh_type not in vh_type_options:
                    current_vh_type = "All"
                vote_history_type = st.selectbox(
                    "Vote History Type",
                    vh_type_options,
                    index=vh_type_options.index(current_vh_type),
                    help="All uses V4A, General uses V4G, and Primary uses V4P.",
                )
                current_range = st.session_state.active_filters.get("vote_history_range", (0, 4))
                if not isinstance(current_range, (list, tuple)) or len(current_range) != 2:
                    current_range = (0, 4)
                vote_history_range = st.slider(
                    "Vote History Range",
                    min_value=0,
                    max_value=4,
                    value=(int(current_range[0]), int(current_range[1])),
                    help="0-4 elections in the selected vote history field.",
                )

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

            with st.expander("Smart Follow-Up", expanded=False):
                contact_status_opts = ["All", "Not Contacted", "Contacted"]
                global_yes_no_opts = ["All", "Yes", "No"]
                support_level_opts = get_global_support_level_options()

                contact_status = st.selectbox(
                    "Contact Status",
                    contact_status_opts,
                    index=contact_status_opts.index(st.session_state.active_filters.get("contact_status", "All")),
                    help="Uses uploaded candidate Street List and Walk Sheet results.",
                )
                global_nh = st.selectbox(
                    "Not Home",
                    global_yes_no_opts,
                    index=global_yes_no_opts.index(st.session_state.active_filters.get("global_nh", "All")),
                )
                global_follow_up = st.selectbox(
                    "Follow-Up",
                    global_yes_no_opts,
                    index=global_yes_no_opts.index(st.session_state.active_filters.get("global_follow_up", "All")),
                )
                current_support = st.session_state.active_filters.get("global_support_level", "All")
                if current_support not in support_level_opts:
                    current_support = "All"
                global_support_level = st.selectbox(
                    "Support Level",
                    support_level_opts,
                    index=support_level_opts.index(current_support),
                )
                st.caption("These filters use uploaded Street List and Walk Sheet tracking data across exports, reports, and turf packets.")

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
                "vote_history_type": vote_history_type,
                "vote_history_range": vote_history_range,
                "mib_applied_pick": mib_applied_pick,
                "mib_ballot_pick": mib_ballot_pick,
                "mb_perm_pick": mb_perm_pick,
                "mb_score_slider": mb_score_slider,
                "new_reg_months": new_reg_months,
                "has_email": has_email,
                "has_landline": has_landline,
                "has_mobile": has_mobile,
                "contact_status": contact_status,
                "global_nh": global_nh,
                "global_follow_up": global_follow_up,
                "global_support_level": global_support_level,
            }
            st.session_state.filters_applied = True
            st.rerun()

        divider()
        with st.expander("⚡ Quick Select Campaign Lists", expanded=False):
            st.caption("These buttons keep your existing geography and voter filters, but quickly set the Smart Follow-Up filters.")
            qs_row1 = st.columns(2, gap="small")
            with qs_row1[0]:
                if st.button("Re-Knock List", use_container_width=True, key="qs_reknock"):
                    apply_followup_preset("Re-Knock List")
            with qs_row1[1]:
                if st.button("Follow-Up List", use_container_width=True, key="qs_followup"):
                    apply_followup_preset("Follow-Up List")

            qs_row2 = st.columns(2, gap="small")
            with qs_row2[0]:
                if st.button("GOTV Supporters", use_container_width=True, key="qs_gotv"):
                    apply_followup_preset("GOTV Supporters")
            with qs_row2[1]:
                if st.button("Undecided Persuasion", use_container_width=True, key="qs_undecided"):
                    apply_followup_preset("Undecided Persuasion")

            qs_row3 = st.columns(2, gap="small")
            with qs_row3[0]:
                if st.button("Yard Sign Follow-Up", use_container_width=True, key="qs_yardsign"):
                    apply_followup_preset("Yard Sign Follow-Up")
            with qs_row3[1]:
                if st.button("Clear Quick Select", use_container_width=True, key="qs_clear"):
                    apply_followup_preset("Clear")

        with st.expander("💾 Saved Universes", expanded=False):
            store_label = get_saved_universe_store_label()
            if store_label == "Cloudflare R2":
                st.caption("Saved universes are stored in persistent Cloudflare R2 storage.")
            else:
                st.caption("Saved universes are using local fallback storage. Add R2 write secrets to keep them across restarts.")

            saved_universes = load_saved_universes()
            st.session_state["saved_universes"] = saved_universes
            universe_names = list(saved_universes.keys())

            if universe_names:
                selected_sidebar_universe = st.selectbox(
                    "Saved Universes",
                    universe_names,
                    key="sidebar_saved_universe_name",
                )
                universe_info = saved_universes[selected_sidebar_universe]
                st.caption(
                    f"Saved: {universe_info.get('saved_at', '')} | Count: {int(universe_info.get('count', 0)):,}"
                )
                st.caption(universe_info.get("summary", "No filters"))
                load_col, delete_col = st.columns(2, gap="small")
                with load_col:
                    if st.button("Load Universe", use_container_width=True, key="load_sidebar_universe"):
                        st.session_state.active_filters = universe_info.get("filters", {})
                        st.session_state.filters_applied = False
                        st.success(f"Loaded universe: {selected_sidebar_universe}")
                        st.rerun()
                with delete_col:
                    if st.button("Delete Universe", use_container_width=True, key="delete_sidebar_universe"):
                        saved_universes.pop(selected_sidebar_universe, None)
                        save_saved_universes(saved_universes)
                        st.session_state["saved_universes"] = saved_universes
                        st.success(f"Deleted universe: {selected_sidebar_universe}")
                        st.rerun()
            else:
                st.caption("No saved universes yet.")

            save_name = st.text_input(
                "Save current filters as",
                key="save_universe_name_sidebar",
                placeholder="Example: GOTV Democrats Week 1",
            )
            if st.button("Save Current Universe", use_container_width=True, key="save_sidebar_universe"):
                universe_name = save_name.strip()
                if universe_name:
                    current_filters = st.session_state.get("active_filters", {})
                    saved_universes = load_saved_universes()
                    saved_universes[universe_name] = {
                        "filters": current_filters,
                        "saved_at": datetime.now().strftime("%Y-%m-%d %I:%M %p"),
                        "count": int(query_metrics(current_filters, st.session_state.get("columns", [])).get("voters", 0)),
                        "summary": summarize_universe_filters(current_filters),
                    }
                    save_saved_universes(saved_universes)
                    st.session_state["saved_universes"] = saved_universes
                    st.success(f"Saved universe: {universe_name}")
                    st.rerun()
                else:
                    st.warning("Enter a universe name first.")


if not st.session_state.data_loaded:
    zeros = [("Voters", "0"), ("Households", "0"), ("Emails", "0"), ("Mobiles", "0"), ("Unique Precincts", "0")]
    metric_cols = st.columns(5, gap="small")
    for col, (label, value) in zip(metric_cols, zeros):
        with col:
            st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)
    divider()
    st.markdown('<div class="section-card empty-shell"><div class="small-header">Ready to load</div><div class="tiny-muted">Click <strong>Load Voter Data</strong> in the sidebar to open the R2 index shards with DuckDB.</div></div>', unsafe_allow_html=True)
    st.stop()

if not st.session_state.filters_applied:
    zeros = [("Voters", "0"), ("Households", "0"), ("Emails", "0"), ("Mobiles", "0"), ("Unique Precincts", "0")]
    metric_cols = st.columns(5, gap="small")
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
    large_filter_mode = use_large_filter_mode(active, columns)
    followup_stats = query_dashboard_followup_stats(active)

    if large_filter_mode:
        party_df = pd.DataFrame(columns=["Party", "Count"])
        gender_df = pd.DataFrame(columns=["Gender", "Count"])
        age_df = pd.DataFrame(columns=["Age Range", "Count"])
        area_choices = []
    else:
        party_df = query_chart(active, columns, "_PartyNorm", "Party")
        gender_df = query_chart(active, columns, "_Gender", "Gender")
        age_df = query_chart(active, columns, "_AgeRange", "Age Range")
        area_choices = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in columns]

metric_cols = st.columns(5, gap="small")
metric_values = [
    ("Voters", f"{safe_int(metrics.get('voters')):,}"),
    ("Households", f"{safe_int(metrics.get('households')):,}"),
    ("Emails", f"{safe_int(metrics.get('emails')):,}"),
    ("Mobiles", f"{safe_int(metrics.get('mobiles')):,}"),
    ("Unique Precincts", f"{safe_int(metrics.get('unique_precincts')):,}"),
]
for col, (label, value) in zip(metric_cols, metric_values):
    with col:
        st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)

campaign_cols = st.columns(4, gap="small")
campaign_values = [
    ("Contacted", f"{safe_int(followup_stats.get('contacted_pct'))}%", f"{safe_int(followup_stats.get('contacted_count')):,} voters"),
    ("Not Home", f"{safe_int(followup_stats.get('nh_pct'))}%", f"{safe_int(followup_stats.get('nh_count')):,} voters"),
    ("Follow-Up", f"{safe_int(followup_stats.get('followup_pct'))}%", f"{safe_int(followup_stats.get('followup_count')):,} voters"),
    ("Undecided", f"{safe_int(followup_stats.get('undecided_pct'))}%", f"{safe_int(followup_stats.get('undecided_count')):,} voters"),
]
for col, (label, value, subvalue) in zip(campaign_cols, campaign_values):
    with col:
        st.markdown(
            f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div><div class="tiny-muted">{subvalue}</div></div>',
            unsafe_allow_html=True
        )

divider()

if large_filter_mode:
    st.warning("Large statewide filter detected. To keep the app stable, some detail-heavy tracking calculations are temporarily simplified until you narrow the universe.")

dashboard_tabs = st.tabs(["Overview", "Contact Tracking", "Output Center", "Voter Lookup"])

with dashboard_tabs[0]:
    if large_filter_mode:
        st.info("Summary-only mode is active for this large statewide filter. Narrow by county, municipality, or precinct to restore charts and grouped tables.")
        summary_only_df = pd.DataFrame([
            {"Metric": "Voters", "Value": f"{safe_int(metrics.get('voters')):,}"},
            {"Metric": "Households", "Value": f"{safe_int(metrics.get('households')):,}"},
            {"Metric": "Emails", "Value": f"{safe_int(metrics.get('emails')):,}"},
            {"Metric": "Mobiles", "Value": f"{safe_int(metrics.get('mobiles')):,}"},
            {"Metric": "Unique Precincts", "Value": f"{safe_int(metrics.get('unique_precincts')):,}"},
        ])
        st.dataframe(summary_only_df, use_container_width=True, hide_index=True)
    else:
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
            selected_area = st.selectbox("Area", area_choices, label_visibility="collapsed", key="overview_area_group")
            area_df = query_area_summary(active, columns, selected_area).copy()
            area_df["Individuals"] = pd.to_numeric(area_df["Individuals"], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
            area_df["Households"] = pd.to_numeric(area_df["Households"], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
            st.dataframe(area_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No area fields available.")
        st.markdown('</div>', unsafe_allow_html=True)

with dashboard_tabs[1]:
    if large_filter_mode:
        st.info("Summary-only mode is active for this large statewide filter. Narrow by county, municipality, or precinct to load Contact Tracking details.")
    else:
        tracking_cols = st.columns(2, gap="medium")
        with tracking_cols[0]:
            st.markdown('<div class="table-card">', unsafe_allow_html=True)
            st.markdown('<div class="small-header">Contact Tracking</div>', unsafe_allow_html=True)
            tracking_summary_df = pd.DataFrame([
                {"Metric": "Contacted", "Percent": f"{safe_int(followup_stats.get('contacted_pct'))}%", "Voters": f"{safe_int(followup_stats.get('contacted_count')):,}"},
                {"Metric": "Not Home", "Percent": f"{safe_int(followup_stats.get('nh_pct'))}%", "Voters": f"{safe_int(followup_stats.get('nh_count')):,}"},
                {"Metric": "Follow-Up", "Percent": f"{safe_int(followup_stats.get('followup_pct'))}%", "Voters": f"{safe_int(followup_stats.get('followup_count')):,}"},
            ])
            st.dataframe(tracking_summary_df, use_container_width=True, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
        with tracking_cols[1]:
            st.markdown('<div class="table-card">', unsafe_allow_html=True)
            st.markdown('<div class="small-header">Support Snapshot</div>', unsafe_allow_html=True)
            support_summary_df = pd.DataFrame([
                {"Metric": "Strong Support", "Percent": f"{safe_int(followup_stats.get('strong_pct'))}%", "Voters": f"{safe_int(followup_stats.get('strong_count')):,}"},
                {"Metric": "Undecided", "Percent": f"{safe_int(followup_stats.get('undecided_pct'))}%", "Voters": f"{safe_int(followup_stats.get('undecided_count')):,}"},
            ])
            st.dataframe(support_summary_df, use_container_width=True, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

with dashboard_tabs[2]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="small-header">Output Center</div>', unsafe_allow_html=True)

    if large_filter_mode:
        st.warning("Large statewide universe detected. Interactive outputs are paused here to keep the app stable. Use the statewide summary report below, or narrow geography to restore the normal Output Center.")
        if st.button("Prepare Statewide Summary Report", use_container_width=True):
            with st.spinner("Building statewide summary report..."):
                st.session_state["statewide_summary_report_bytes"] = build_statewide_summary_report_bytes(active, columns)
        if "statewide_summary_report_bytes" in st.session_state and st.session_state["statewide_summary_report_bytes"]:
            st.download_button(
                "Download Statewide Summary Report",
                data=st.session_state["statewide_summary_report_bytes"],
                file_name="candidate_connect_statewide_summary_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        st.caption("This workbook includes Overview, Filters, County, USC, STS, and STH sheets for the current universe.")
    else:
        output_tabs = st.tabs(["Exports", "Reports", "Turf Builder"])
        with output_tabs[0]:
            st.markdown('<div class="small-header">Exports</div>', unsafe_allow_html=True)
            st.caption("CSV files are only built when you click the button for that export type.")
    
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
    
        with output_tabs[1]:
            st.markdown('<div class="small-header">Reports</div>', unsafe_allow_html=True)
            st.caption("Prepare PDFs only when needed to keep the app responsive.")
    
            report_sections = st.tabs(["Summary", "Street List", "Walk Sheet", "Mailing Labels"])
    
            with report_sections[0]:
                st.caption("Builds a clean PDF summary of the current filtered universe with overview counts, selected filters, and party/gender/age breakdowns.")
                summary_cols = st.columns(2, gap="medium")
                with summary_cols[0]:
                    if st.button("Prepare Summary Report PDF", use_container_width=True):
                        with st.spinner("Building Summary Report PDF from current filtered universe..."):
                            pdf_bytes = generate_summary_report_pdf_bytes(active, cols)
                            st.session_state["summary_report_pdf_bytes"] = pdf_bytes
                with summary_cols[1]:
                    if "summary_report_pdf_bytes" in st.session_state and st.session_state["summary_report_pdf_bytes"]:
                        st.download_button(
                            "Download Summary Report PDF",
                            data=st.session_state["summary_report_pdf_bytes"],
                            file_name="candidate_connect_summary_report.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
    
            with report_sections[1]:
                st.caption("Builds a compact precinct-grouped PDF and also supports a Street List Excel tracking sheet so the same list can be used to record F, A, U, NH, and Yard Sign results.")
                upload_cols = st.columns([1, 1.2, 1], gap="medium")
                with upload_cols[0]:
                    st.download_button(
                        "Download Street Results CSV Template",
                        data=get_street_results_template_csv_bytes(),
                        file_name="candidate_connect_street_results_template.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                    st.download_button(
                        "Download Street List Excel Tracking Sheet",
                        data=get_street_results_sheet_bytes(active),
                        file_name="candidate_connect_street_list_tracking.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
                with upload_cols[1]:
                    uploaded_results_file = st.file_uploader(
                        "Upload Street Results File",
                        type=["csv", "xlsx"],
                        key="street_results_upload",
                        help="Upload either the Street List Excel tracking sheet or a CSV using PA ID Number plus F, A, U, NH, and Yard Sign columns.",
                    )
                    if uploaded_results_file is not None:
                        upload_sig = f"{uploaded_results_file.name}:{getattr(uploaded_results_file, 'size', 0)}"
                        if st.session_state.get("street_results_upload_sig") != upload_sig:
                            try:
                                if str(uploaded_results_file.name).lower().endswith(".xlsx"):
                                    raw_upload_df = pd.read_excel(uploaded_results_file, dtype=str).fillna("")
                                    normalized_cols = [re.sub(r"[^a-z0-9]+", "", str(c).strip().lower()) for c in raw_upload_df.columns]
                                    if "paidnumber" not in normalized_cols:
                                        try:
                                            raw_upload_df = pd.read_excel(uploaded_results_file, dtype=str, header=4).fillna("")
                                        except Exception:
                                            uploaded_results_file.seek(0)
                                            raw_upload_df = pd.read_excel(uploaded_results_file, dtype=str).fillna("")
                                    uploaded_results_file.seek(0)
                                else:
                                    raw_upload_df = pd.read_csv(uploaded_results_file, dtype=str).fillna("")
                                standardized_upload_df = standardize_uploaded_street_results(raw_upload_df)
                                if standardized_upload_df.empty:
                                    st.warning("No usable PA ID Number column was found in the uploaded file.")
                                else:
                                    st.session_state["street_results_df"] = standardized_upload_df
                                    st.session_state["street_results_upload_sig"] = upload_sig
                                    st.session_state["street_results_upload_name"] = uploaded_results_file.name
                                    st.success(f"Loaded {len(standardized_upload_df):,} street-result rows.")
                            except Exception as exc:
                                st.error(f"Could not read the street results file: {exc}")
                with upload_cols[2]:
                    loaded_results = st.session_state.get("street_results_df")
                    if isinstance(loaded_results, pd.DataFrame) and not loaded_results.empty:
                        st.caption(f"Loaded rows: {len(loaded_results):,}")
                        st.caption(f"Source: {st.session_state.get('street_results_upload_name', 'uploaded CSV')}")
                        if st.button("Clear Uploaded Street Results", use_container_width=True):
                            st.session_state["street_results_df"] = pd.DataFrame(columns=["PA ID Number", "F", "A", "U", "NH", "Yard Sign", "Notes"])
                            st.session_state["street_results_filters"] = {}
                            st.session_state.pop("street_results_upload_sig", None)
                            st.session_state.pop("street_results_upload_name", None)
                            st.rerun()
                    else:
                        st.caption("No street results uploaded yet.")
    
                loaded_results = st.session_state.get("street_results_df")
                if isinstance(loaded_results, pd.DataFrame) and not loaded_results.empty:
                    st.caption("These tracking filters only affect the Street List outputs, so you can reprint or re-export candidate follow-up lists without changing the dashboard counts.")
                    filter_defaults = st.session_state.get("street_results_filters", {}) or {}
                    street_filter_cols = st.columns(5, gap="small")
                    street_results_filters = {}
                    for col, field in zip(street_filter_cols, ["F", "A", "U", "NH", "Yard Sign"]):
                        with col:
                            street_results_filters[field] = st.selectbox(
                                field,
                                ["All", "Marked", "Unmarked"],
                                index=["All", "Marked", "Unmarked"].index(filter_defaults.get(field, "All")),
                                key=f"street_results_filter_{field}",
                            )
                    st.session_state["street_results_filters"] = street_results_filters
                else:
                    st.caption("Download the Street List Excel tracking sheet if you want a ready-to-use file with F, A, U, NH, Yard Sign, and Notes columns, then upload it back after results are entered.")
    
                pdf_cols = st.columns(2, gap="medium")
                with pdf_cols[0]:
                    if st.button("Prepare Street List PDF", use_container_width=True):
                        with st.spinner("Building Street List PDF from filtered detail shards..."):
                            pdf_bytes = generate_street_list_pdf_bytes(active)
                            st.session_state["street_pdf_bytes"] = pdf_bytes
                with pdf_cols[1]:
                    if "street_pdf_bytes" in st.session_state and st.session_state["street_pdf_bytes"]:
                        st.download_button(
                            "Download Street List PDF",
                            data=st.session_state["street_pdf_bytes"],
                            file_name="candidate_connect_street_list.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
    
            with report_sections[2]:
                st.caption("Builds a volunteer-friendly walk sheet and supports a tracking workbook that can be uploaded back by PA ID.")
                upload_cols = st.columns([1, 1.15, 1], gap="medium")
                with upload_cols[0]:
                    st.download_button(
                        "Download Walk Sheet Tracking Template",
                        data=get_walk_sheet_tracking_template_csv_bytes(),
                        file_name="candidate_connect_walk_sheet_tracking_template.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                    if st.button("Prepare Walk Sheet Excel Tracking Sheet", use_container_width=True):
                        with st.spinner("Building Walk Sheet Excel tracking sheet from filtered detail shards..."):
                            excel_bytes = build_walk_sheet_tracking_excel_bytes(active)
                            st.session_state["walk_sheet_tracking_excel_bytes"] = excel_bytes
                    if "walk_sheet_tracking_excel_bytes" in st.session_state and st.session_state["walk_sheet_tracking_excel_bytes"]:
                        st.download_button(
                            "Download Walk Sheet Excel Tracking Sheet",
                            data=st.session_state["walk_sheet_tracking_excel_bytes"],
                            file_name="candidate_connect_walk_sheet_tracking.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )
                with upload_cols[1]:
                    uploaded_walk_file = st.file_uploader(
                        "Upload Walk Sheet Results",
                        type=["csv", "xlsx"],
                        key="walk_results_upload",
                        help="Upload a completed Walk Sheet tracking workbook or CSV using PA ID Number plus Contacted, Result, Support Level, Follow-Up, and Notes columns.",
                    )
                    if uploaded_walk_file is not None:
                        upload_sig = f"{uploaded_walk_file.name}:{getattr(uploaded_walk_file, 'size', 0)}"
                        if st.session_state.get("walk_results_upload_sig") != upload_sig:
                            try:
                                if str(uploaded_walk_file.name).lower().endswith(".xlsx"):
                                    raw_upload_df = pd.read_excel(uploaded_walk_file, dtype=str).fillna("")
                                    normalized_cols = [re.sub(r"[^a-z0-9]+", "", str(c).strip().lower()) for c in raw_upload_df.columns]
                                    if "paidnumber" not in normalized_cols:
                                        try:
                                            raw_upload_df = pd.read_excel(uploaded_walk_file, dtype=str, header=4).fillna("")
                                        except Exception:
                                            uploaded_walk_file.seek(0)
                                            raw_upload_df = pd.read_excel(uploaded_walk_file, dtype=str).fillna("")
                                    uploaded_walk_file.seek(0)
                                else:
                                    raw_upload_df = pd.read_csv(uploaded_walk_file, dtype=str).fillna("")
                                standardized_upload_df = standardize_uploaded_walk_results(raw_upload_df)
                                if standardized_upload_df.empty:
                                    st.warning("No usable PA ID Number column was found in the uploaded Walk Sheet file.")
                                else:
                                    st.session_state["walk_results_df"] = standardized_upload_df
                                    st.session_state["walk_results_upload_sig"] = upload_sig
                                    st.session_state["walk_results_upload_name"] = uploaded_walk_file.name
                                    st.success(f"Loaded {len(standardized_upload_df):,} walk-result rows.")
                            except Exception as exc:
                                st.error(f"Could not read the Walk Sheet results file: {exc}")
                with upload_cols[2]:
                    loaded_walk_results = st.session_state.get("walk_results_df")
                    if isinstance(loaded_walk_results, pd.DataFrame) and not loaded_walk_results.empty:
                        st.caption(f"Loaded rows: {len(loaded_walk_results):,}")
                        st.caption(f"Source: {st.session_state.get('walk_results_upload_name', 'uploaded file')}")
                        if st.button("Clear Uploaded Walk Sheet Results", use_container_width=True):
                            st.session_state["walk_results_df"] = pd.DataFrame(columns=["PA ID Number", "Contacted", "Result", "Support Level", "Follow-Up", "Notes"])
                            st.session_state["walk_results_filters"] = {}
                            st.session_state.pop("walk_results_upload_sig", None)
                            st.session_state.pop("walk_results_upload_name", None)
                            st.rerun()
                    else:
                        st.caption("No Walk Sheet results uploaded yet.")
    
                loaded_walk_results = st.session_state.get("walk_results_df")
                if isinstance(loaded_walk_results, pd.DataFrame) and not loaded_walk_results.empty:
                    st.caption("These tracking filters apply only to the Walk Sheet PDF, so you can rebuild volunteer re-knock or follow-up sheets without changing the dashboard counts.")
                    filter_defaults = st.session_state.get("walk_results_filters", {}) or {}
                    walk_filter_cols = st.columns(4, gap="small")
                    with walk_filter_cols[0]:
                        contacted_filter = st.selectbox(
                            "Contacted",
                            ["All", "Marked", "Unmarked"],
                            index=["All", "Marked", "Unmarked"].index(filter_defaults.get("Contacted", "All")),
                            key="walk_results_filter_contacted",
                        )
                    with walk_filter_cols[1]:
                        not_home_filter = st.selectbox(
                            "Not Home",
                            ["All", "Marked", "Unmarked"],
                            index=["All", "Marked", "Unmarked"].index(filter_defaults.get("Not Home", "All")),
                            key="walk_results_filter_not_home",
                        )
                    with walk_filter_cols[2]:
                        followup_filter = st.selectbox(
                            "Follow-Up",
                            ["All", "Marked", "Unmarked"],
                            index=["All", "Marked", "Unmarked"].index(filter_defaults.get("Follow-Up", "All")),
                            key="walk_results_filter_followup",
                        )
                    support_options = ["All"] + sorted(
                        {normalize_export_text(v) for v in loaded_walk_results["Support Level"].tolist() if normalize_export_text(v)}
                    )
                    default_support = filter_defaults.get("Support Level", "All")
                    if default_support not in support_options:
                        default_support = "All"
                    with walk_filter_cols[3]:
                        support_filter = st.selectbox(
                            "Support Level",
                            support_options,
                            index=support_options.index(default_support),
                            key="walk_results_filter_support",
                        )
                    st.session_state["walk_results_filters"] = {
                        "Contacted": contacted_filter,
                        "Not Home": not_home_filter,
                        "Follow-Up": followup_filter,
                        "Support Level": support_filter,
                    }
                else:
                    st.caption("Download the Walk Sheet Excel tracking sheet if you want a ready-to-use file with Contacted, Result, Support Level, Follow-Up, and Notes columns, then upload it back after results are entered.")
    
                walk_cols = st.columns(2, gap="medium")
                with walk_cols[0]:
                    if st.button("Prepare Walk Sheet PDF", use_container_width=True):
                        with st.spinner("Building Walk Sheet PDF from filtered detail shards..."):
                            pdf_bytes = generate_walk_sheet_pdf_bytes(active)
                            st.session_state["walk_sheet_pdf_bytes"] = pdf_bytes
                with walk_cols[1]:
                    if "walk_sheet_pdf_bytes" in st.session_state and st.session_state["walk_sheet_pdf_bytes"]:
                        st.download_button(
                            "Download Walk Sheet PDF",
                            data=st.session_state["walk_sheet_pdf_bytes"],
                            file_name="candidate_connect_walk_sheet.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
    
            with report_sections[3]:
                st.caption("Builds a print-ready Avery 5160-style PDF label sheet from the current mail export universe.")
                label_mode = st.radio(
                    "Label Mode",
                    ["Householded", "Individual"],
                    horizontal=True,
                    key="mail_labels_mode",
                )
                label_cols = st.columns(2, gap="medium")
                with label_cols[0]:
                    if st.button("Prepare Mailing Labels PDF", use_container_width=True):
                        with st.spinner("Building mailing labels PDF from filtered detail shards..."):
                            pdf_bytes = generate_mailing_labels_pdf_bytes(active, householded=(label_mode == "Householded"))
                            st.session_state["mailing_labels_pdf_bytes"] = pdf_bytes
                            st.session_state["mailing_labels_pdf_mode"] = label_mode
                with label_cols[1]:
                    if "mailing_labels_pdf_bytes" in st.session_state and st.session_state["mailing_labels_pdf_bytes"]:
                        suffix = "householded" if st.session_state.get("mailing_labels_pdf_mode") == "Householded" else "individual"
                        st.download_button(
                            "Download Mailing Labels PDF",
                            data=st.session_state["mailing_labels_pdf_bytes"],
                            file_name=f"candidate_connect_mailing_labels_{suffix}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
    
    
        with output_tabs[2]:
            st.markdown('<div class="small-header">Turf Builder</div>', unsafe_allow_html=True)
            st.caption("Split the current filtered universe into turf packets and download a ZIP with per-turf CSVs and Walk Sheet PDFs.")
    
            turf_mode_labels = {
                "Target Doors": "doors",
                "Target Voters": "voters",
                "By Precinct": "precinct",
                "By Municipality": "municipality",
            }
    
            turf_mode = st.selectbox(
                "Split Method",
                list(turf_mode_labels.keys()),
                key="turf_mode_select",
            )
    
            if turf_mode in ["Target Doors", "Target Voters"]:
                default_size = 50 if turf_mode == "Target Doors" else 100
                turf_target_size = st.slider(
                    "Target Size Per Turf",
                    min_value=10,
                    max_value=500,
                    value=default_size,
                    step=5,
                    key="turf_target_size_slider",
                )
                st.caption(
                    "Target Doors uses households/address groups. Target Voters uses total voter records. "
                    "Packets are built sequentially from the current filtered universe."
                )
            else:
                turf_target_size = 0
                st.caption("This will create one turf per selected precinct or municipality in the current filtered universe.")
    
            assign_cols = st.columns(3, gap="medium")
            with assign_cols[0]:
                turf_packet_label = st.text_input(
                    "Packet Label",
                    value="",
                    placeholder="Week 1 - Team A",
                    key="turf_packet_label_input",
                )
            with assign_cols[1]:
                turf_volunteer_name = st.text_input(
                    "Volunteer Name",
                    value="",
                    placeholder="Volunteer or team name",
                    key="turf_volunteer_name_input",
                )
            with assign_cols[2]:
                turf_packet_date = st.date_input(
                    "Packet Date",
                    value=datetime.now().date(),
                    key="turf_packet_date_input",
                )
    
            perf_cols = st.columns([1.2, 1, 1], gap="medium")
            with perf_cols[0]:
                turf_output_mode = st.selectbox(
                    "Output Type",
                    ["CSV + Walk Sheet PDFs", "CSV Only (faster)"],
                    key="turf_output_mode_select",
                )
            with perf_cols[1]:
                turf_limit_packets = st.number_input(
                    "Limit Turf Packets",
                    min_value=0,
                    value=0,
                    step=1,
                    help="0 means build all turfs. Use a smaller number for quick tests.",
                    key="turf_limit_packets_input",
                )
            with perf_cols[2]:
                st.markdown("")
                st.caption("CSV Only is fastest. By Precinct and By Municipality can take much longer when PDFs are included.")
    
            if turf_output_mode == "CSV + Walk Sheet PDFs" and turf_mode in ["By Precinct", "By Municipality"]:
                st.warning("This can take longer because the app creates one PDF per turf. For the fastest build, choose CSV Only or set a small turf limit first.")
    
            turf_cols = st.columns(2, gap="medium")
            with turf_cols[0]:
                if st.button("Prepare Turf Packet ZIP", use_container_width=True):
                    spinner_text = "Building turf packet ZIP from filtered detail shards..."
                    if turf_output_mode == "CSV + Walk Sheet PDFs":
                        spinner_text = "Building turf packets and walk sheets from filtered detail shards..."
                    with st.spinner(spinner_text):
                        zip_bytes = build_turf_packet_zip(
                            active_filters=active,
                            mode=turf_mode_labels[turf_mode],
                            target_size=turf_target_size,
                            volunteer_name=turf_volunteer_name,
                            packet_label=turf_packet_label,
                            packet_date=turf_packet_date.strftime("%Y-%m-%d") if turf_packet_date else "",
                            include_walksheets=(turf_output_mode == "CSV + Walk Sheet PDFs"),
                            max_turfs=int(turf_limit_packets or 0),
                        )
                        st.session_state["turf_packet_zip_bytes"] = zip_bytes
                        st.session_state["turf_packet_mode"] = turf_mode
                        st.session_state["turf_packet_label"] = turf_packet_label
                        st.session_state["turf_output_mode"] = turf_output_mode
                        st.session_state["turf_limit_packets"] = int(turf_limit_packets or 0)
            with turf_cols[1]:
                if "turf_packet_zip_bytes" in st.session_state and st.session_state["turf_packet_zip_bytes"]:
                    mode_slug = normalize_export_text(st.session_state.get("turf_packet_mode", "turf_packets")).lower().replace(" ", "_")
                    label_slug = sanitize_filename_part(st.session_state.get("turf_packet_label", ""))
                    output_slug = "csv_only" if st.session_state.get("turf_output_mode") == "CSV Only (faster)" else "csv_and_pdfs"
                    limit_val = int(st.session_state.get("turf_limit_packets", 0) or 0)
                    limit_slug = f"_first_{limit_val}" if limit_val > 0 else ""
                    file_stub = f"candidate_connect_turf_packets_{label_slug}_{mode_slug}_{output_slug}{limit_slug}" if label_slug else f"candidate_connect_turf_packets_{mode_slug}_{output_slug}{limit_slug}"
                    st.download_button(
                        "Download Turf Packet ZIP",
                        data=st.session_state["turf_packet_zip_bytes"],
                        file_name=f"{file_stub}.zip",
                        mime="application/zip",
                        use_container_width=True,
                    )
    
    
        st.markdown('</div>', unsafe_allow_html=True)


with dashboard_tabs[3]:
    if large_filter_mode:
        st.info("Voter Lookup is available here even in large-universe mode, but it searches inside the current filtered universe. Narrow geography if you want a smaller result set.")
    render_voter_lookup_tab(active, columns)
