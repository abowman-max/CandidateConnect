import streamlit as st
import pandas as pd
import gdown
from pathlib import Path
from io import BytesIO

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


def clean_text(val):
    if pd.isna(val):
        return ""
    text = str(val).strip()
    return "" if text.lower() in {"nan", "none"} else text


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
    return household_key(frame).nunique() if len(frame) else 0


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


def first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
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


def resolve_voter_id_column(frame: pd.DataFrame) -> str | None:
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

st.subheader("Exports")
ex1, ex2, ex3 = st.columns([1.2, 1, 1])

with ex1:
    household_mode = st.radio("Mailing Mode", ["Not Householded", "Householded"], horizontal=True)

mail_df = build_mail_export(filtered, householded=(household_mode == "Householded"))
texting_df = build_texting_export(filtered)

mail_csv = mail_df.to_csv(index=False).encode("utf-8")
mail_xlsx = dataframe_to_excel_bytes(mail_df, "Mail File")
texting_csv = texting_df.to_csv(index=False).encode("utf-8")

with ex2:
    st.download_button(
        "Download Mail CSV",
        data=mail_csv,
        file_name="candidate_connect_mail_file.csv",
        mime="text/csv",
        use_container_width=True,
    )
    st.download_button(
        "Download Mail Excel",
        data=mail_xlsx,
        file_name="candidate_connect_mail_file.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

with ex3:
    st.download_button(
        "Download Texting CSV",
        data=texting_csv,
        file_name="candidate_connect_texting_file.csv",
        mime="text/csv",
        use_container_width=True,
    )
    st.caption(f"Mail rows: {len(mail_df):,} | Text rows: {len(texting_df):,}")

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
