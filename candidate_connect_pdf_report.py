
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas


PAGE_SIZE = landscape(letter)
PAGE_WIDTH, PAGE_HEIGHT = PAGE_SIZE

MARGIN_LEFT = 24
MARGIN_RIGHT = 24
TOP_HEADER_Y = PAGE_HEIGHT - 20
HEADER_LINE_Y = PAGE_HEIGHT - 64
FOOTER_Y = 16

COLOR_MAROON = colors.HexColor("#8f1021")
COLOR_MAROON_DARK = colors.HexColor("#750d1a")
COLOR_TABLE_GRID = colors.HexColor("#c8c1c1")
COLOR_LIGHT_ROW = colors.HexColor("#f1ebeb")
COLOR_LIGHT_BAND = colors.HexColor("#e7d7d7")
COLOR_TEXT = colors.HexColor("#333333")
COLOR_MUTED = colors.HexColor("#666666")
COLOR_SOFT_BOX = colors.HexColor("#efe8e8")


class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, created_text: str = "", logo_paths: Dict[str, str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []
        self.created_text = created_text
        self.logo_paths = logo_paths or {}

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self._draw_footer(total_pages)
            super().showPage()
        super().save()

    def _draw_footer(self, total_pages: int):
        self.setFont("Helvetica", 11)
        self.setFillColor(COLOR_MUTED)
        self.drawString(MARGIN_LEFT, FOOTER_Y, f"{self._pageNumber} of {total_pages}")
        self.drawRightString(PAGE_WIDTH - MARGIN_RIGHT, FOOTER_Y, self.created_text)


def clean_text(val) -> str:
    if pd.isna(val):
        return ""
    text = str(val).strip()
    if text.lower() in {"", "nan", "none", "nat"}:
        return ""
    return text


def smart_title(val) -> str:
    text = clean_text(val)
    if not text:
        return ""
    return " ".join(word.capitalize() for word in text.replace("_", " ").split())


def first_existing_column(frame: pd.DataFrame, candidates: Iterable[str]):
    for col in candidates:
        if col in frame.columns:
            return col
    lowered = {str(c).strip().lower(): c for c in frame.columns}
    for col in candidates:
        match = lowered.get(str(col).strip().lower())
        if match is not None:
            return match
    return None


def count_households(frame: pd.DataFrame) -> int:
    if len(frame) == 0:
        return 0
    if "HH_ID" in frame.columns:
        hh = frame["HH_ID"].astype(str).str.strip()
        hh = hh.where(hh != "", pd.NA)
        if hh.notna().any():
            return hh.dropna().nunique()
    parts = []
    for col in ["House Number", "Street Name", "Apartment Number"]:
        real = first_existing_column(frame, [col])
        if real:
            parts.append(frame[real].astype(str).fillna(""))
    if not parts:
        return len(frame)
    key = parts[0]
    for p in parts[1:]:
        key = key + "|" + p
    return key.nunique()


def _format_phone_number(raw: str) -> str:
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return raw.strip()


def _phone_from_row(row: pd.Series) -> str:
    mobile_col = next((c for c in ["Mobile", "Cell", "Cell Phone"] if c in row.index), None)
    landline_col = next((c for c in ["Landline", "Phone", "Home Phone"] if c in row.index), None)

    mobile = clean_text(row.get(mobile_col, "")) if mobile_col else ""
    landline = clean_text(row.get(landline_col, "")) if landline_col else ""

    if mobile:
        phone = _format_phone_number(mobile)
        return phone if phone.endswith("(m)") else f"{phone} (m)"
    if landline:
        phone = _format_phone_number(landline)
        return phone if phone.endswith("(l)") else f"{phone} (l)"
    return ""


def _full_name_from_row(row: pd.Series) -> str:
    full_col = first_existing_column(pd.DataFrame(columns=row.index), ["FullName", "Full Name"])
    if full_col:
        full = smart_title(row.get(full_col, ""))
        if full:
            return full
    parts = [
        smart_title(row.get("FirstName", "")),
        smart_title(row.get("MiddleName", "")),
        smart_title(row.get("LastName", "")),
        smart_title(row.get("NameSuffix", "")),
    ]
    return " ".join([p for p in parts if p]).strip()


def _sex_from_row(row: pd.Series) -> str:
    for col in ["Gender", "Sex", "_Gender"]:
        if col in row.index:
            value = clean_text(row.get(col, ""))
            if value:
                return value.upper()[:1]
    return ""


def _simple_yes_no(row: pd.Series, column_candidates: Iterable[str]) -> str:
    col = next((c for c in column_candidates if c in row.index), None)
    if not col:
        return ""
    raw = row.get(col, "")
    if pd.isna(raw):
        return ""
    value = str(raw).strip()
    if not value or value.lower() in {"nan", "none"}:
        return ""
    value_up = value.upper()
    if value_up in {"Y", "YES", "TRUE", "1", "T"}:
        return "Y"
    if value_up in {"N", "NO", "FALSE", "0", "F"}:
        return "N"
    return value


def _row_to_detail(row: pd.Series) -> Dict[str, str]:
    age = ""
    for col in ["_AgeNum", "Age"]:
        if col in row.index and pd.notna(row.get(col)):
            try:
                age = str(int(float(row.get(col))))
            except Exception:
                age = clean_text(row.get(col, ""))
            if age:
                break

    party = clean_text(row.get("Party", ""))
    if party == "O":
        party = "O"

    # F / A / U are display checkboxes only in the report template.
    # Keep them visually empty unless the dataset later gains explicit values.
    f_val = ""
    a_val = ""
    u_val = ""
    yard = _simple_yes_no(row, ["Yard Sign", "YardSign", "YARD_SIGN"])
    mb_perm = _simple_yes_no(row, ["MB_Perm", "MB_PERM", "MB_Pern", "_MBPerm"])

    return {
        "Full Name": _full_name_from_row(row),
        "Phone": _phone_from_row(row),
        "Party": party,
        "Sex": _sex_from_row(row),
        "Age": age,
        "F": f_val,
        "A": a_val,
        "U": u_val,
        "Yard Sign": yard,
        "MB_Perm": mb_perm,
    }


def _build_address_parts(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    house_col = first_existing_column(df, ["House Number", "HouseNumber", "Street Number"])
    street_col = first_existing_column(df, ["Street Name", "StreetName"])
    apt_col = first_existing_column(df, ["Apartment Number", "ApartmentNumber", "Unit", "Apt"])

    df["_HouseNumRaw"] = df[house_col].astype(str).fillna("") if house_col else ""
    df["_HouseNumSort"] = pd.to_numeric(df["_HouseNumRaw"].str.extract(r"(\d+)")[0], errors="coerce")
    if df["_HouseNumSort"].isna().all():
        df["_HouseNumSort"] = range(1, len(df) + 1)
    df["_Street"] = df[street_col].astype(str).fillna("").str.strip() if street_col else ""
    df["_Apt"] = df[apt_col].astype(str).fillna("").str.strip() if apt_col else ""
    df["_AddressLine1"] = (df["_HouseNumRaw"].astype(str) + " " + df["_Street"].astype(str)).str.replace(r"\s+", " ", regex=True).str.strip()

    def merge_address(row):
        base = clean_text(row.get("_AddressLine1", ""))
        apt = clean_text(row.get("_Apt", ""))
        if apt:
            return f"{base} Apt {apt}".strip()
        return base

    df["_AddressLineFull"] = df.apply(merge_address, axis=1)
    df["_Precinct"] = df[first_existing_column(df, ["Precinct"])].astype(str).fillna("").str.strip() if first_existing_column(df, ["Precinct"]) else "All Precincts"
    df["_County"] = df[first_existing_column(df, ["County"])].astype(str).fillna("").str.strip() if first_existing_column(df, ["County"]) else ""
    return df


def build_door_to_door_table(frame: pd.DataFrame) -> pd.DataFrame:
    df = _build_address_parts(frame)

    records = []
    for _, row in df.iterrows():
        detail = _row_to_detail(row)
        records.append({
            "Precinct": clean_text(row.get("_Precinct", "")),
            "Street": clean_text(row.get("_Street", "")),
            "House Number": clean_text(row.get("_HouseNumRaw", "")),
            "Address": clean_text(row.get("_AddressLineFull", "")),
            **detail
        })

    out = pd.DataFrame(records)
    if out.empty:
        return out

    out["_street_sort"] = out["Street"].str.upper()
    out["_house_sort"] = pd.to_numeric(out["House Number"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    out["_name_sort"] = out["Full Name"].astype(str).str.upper()
    out = out.sort_values(
        by=["Precinct", "_street_sort", "_house_sort", "Address", "_name_sort"],
        ascending=[True, True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)
    return out.drop(columns=["_street_sort", "_house_sort", "_name_sort"])


def _draw_page_header(c: canvas.Canvas, page_title: str, logo_paths: Dict[str, str], show_center_title: bool = True):
    cc_logo = logo_paths.get("candidate_connect")
    tss_logo = logo_paths.get("tss")

    if cc_logo and Path(cc_logo).exists():
        c.drawImage(ImageReader(cc_logo), 60, PAGE_HEIGHT - 45, width=122, height=40, mask="auto")
    if tss_logo and Path(tss_logo).exists():
        c.setFont("Helvetica-Bold", 15)
        c.setFillColor(COLOR_MUTED)
        c.drawString(PAGE_WIDTH - 152, PAGE_HEIGHT - 25, "Powered By")
        c.drawImage(ImageReader(tss_logo), PAGE_WIDTH - 150, PAGE_HEIGHT - 52, width=70, height=22, mask="auto")

    c.setStrokeColor(colors.HexColor("#4e555b"))
    c.setLineWidth(1)
    c.line(MARGIN_LEFT, HEADER_LINE_Y, PAGE_WIDTH - MARGIN_RIGHT, HEADER_LINE_Y)

    if show_center_title and page_title:
        c.setFont("Helvetica-Bold", 21)
        c.setFillColor(COLOR_TEXT)
        c.drawCentredString(PAGE_WIDTH / 2, PAGE_HEIGHT - 20, page_title)


def _fit_centered_text(c: canvas.Canvas, text: str, y: float, max_width: float, font_name: str = "Helvetica-Bold", start_size: int = 21, min_size: int = 14):
    size = start_size
    while size >= min_size and stringWidth(text, font_name, size) > max_width:
        size -= 1
    c.setFont(font_name, size)
    c.drawCentredString(PAGE_WIDTH / 2, y, text)


def _draw_cover_page(c: canvas.Canvas, filtered: pd.DataFrame, report_title: str, created_date: str, logo_paths: Dict[str, str]):
    _draw_page_header(c, "", logo_paths, show_center_title=False)

    cc_logo = logo_paths.get("candidate_connect")
    if cc_logo and Path(cc_logo).exists():
        c.drawImage(ImageReader(cc_logo), PAGE_WIDTH / 2 - 140, PAGE_HEIGHT / 2 + 65, width=280, height=100, mask="auto")

    county_val = ""
    county_col = first_existing_column(filtered, ["County"])
    if county_col and len(filtered):
        non_blank = [smart_title(v) for v in filtered[county_col].dropna().astype(str).tolist() if clean_text(v)]
        if non_blank:
            unique = sorted(set(non_blank))
            county_val = unique[0] if len(unique) == 1 else "Multi-County"

    subtitle = report_title or "Door-to-Door Street List"
    households = count_households(filtered)
    individuals = len(filtered)

    c.setFillColor(COLOR_TEXT)
    c.setFont("Helvetica-Bold", 30)
    c.drawCentredString(PAGE_WIDTH / 2, PAGE_HEIGHT / 2 + 5, county_val or "Candidate Connect")

    c.setFont("Helvetica", 22)
    c.drawCentredString(PAGE_WIDTH / 2, PAGE_HEIGHT / 2 - 38, created_date)

    c.setFont("Helvetica", 20)
    c.drawCentredString(PAGE_WIDTH / 2, PAGE_HEIGHT / 2 - 82, subtitle)

    box_w = 410
    box_h = 82
    box_x = PAGE_WIDTH / 2 - box_w / 2
    box_y = PAGE_HEIGHT / 2 - 185
    c.setFillColor(COLOR_SOFT_BOX)
    c.roundRect(box_x, box_y, box_w, box_h, 14, stroke=0, fill=1)

    c.setFillColor(COLOR_MAROON)
    c.setFont("Helvetica-Bold", 22)
    c.drawCentredString(PAGE_WIDTH / 2 - 86, box_y + 36, f"Individuals: {individuals:,}")
    c.drawCentredString(PAGE_WIDTH / 2 + 88, box_y + 36, f"Households: {households:,}")


def _draw_counts_summary_pages(c: canvas.Canvas, precinct_counts: pd.DataFrame, logo_paths: Dict[str, str]):
    rows_per_page = 20
    pages = [precinct_counts.iloc[i:i+rows_per_page].copy() for i in range(0, len(precinct_counts), rows_per_page)]
    if not pages:
        pages = [pd.DataFrame(columns=["Precinct", "Individuals", "Households"])]

    for page_df in pages:
        c.bookmarkPage("precinct_counts_summary" if c.getPageNumber() == 2 else f"precinct_counts_summary_{c.getPageNumber()}")
        if c.getPageNumber() == 2:
            c.addOutlineEntry("Precinct Counts Summary", "precinct_counts_summary", level=0, closed=False)
        _draw_page_header(c, "Precinct Counts Summary", logo_paths, show_center_title=True)

        c.setFillColor(COLOR_TEXT)
        c.setFont("Helvetica-Bold", 24)
        c.drawString(MARGIN_LEFT, PAGE_HEIGHT - 84, "Precinct Counts Summary")

        x = MARGIN_LEFT
        y = PAGE_HEIGHT - 117
        widths = [460, 112, 122]
        row_h = 26

        c.setFillColor(COLOR_MAROON)
        c.rect(x, y, sum(widths), row_h, stroke=0, fill=1)
        c.setFont("Helvetica-Bold", 15)
        c.setFillColor(colors.white)
        c.drawString(x + 10, y + 8, "Precinct")
        c.drawCentredString(x + widths[0] + widths[1] / 2, y + 8, "Individuals")
        c.drawCentredString(x + widths[0] + widths[1] + widths[2] / 2, y + 8, "Households")

        y -= row_h
        c.setFont("Helvetica", 14)
        for idx, (_, row) in enumerate(page_df.iterrows()):
            fill_color = COLOR_LIGHT_BAND if idx % 7 == 0 else COLOR_LIGHT_ROW
            c.setFillColor(fill_color)
            c.rect(x, y, sum(widths), row_h, stroke=0, fill=1)

            c.setStrokeColor(COLOR_TABLE_GRID)
            c.rect(x, y, sum(widths), row_h, stroke=1, fill=0)
            c.line(x + widths[0], y, x + widths[0], y + row_h)
            c.line(x + widths[0] + widths[1], y, x + widths[0] + widths[1], y + row_h)

            c.setFillColor(COLOR_TEXT)
            c.drawString(x + 10, y + 7, clean_text(row.get("Precinct", ""))[:68])
            c.drawCentredString(x + widths[0] + widths[1] / 2, y + 7, f"{int(row.get('Individuals', 0)):,}")
            c.drawCentredString(x + widths[0] + widths[1] + widths[2] / 2, y + 7, f"{int(row.get('Households', 0)):,}")
            y -= row_h

        c.showPage()


def _checkbox(c: canvas.Canvas, x: float, y: float, size: float = 10):
    c.setLineWidth(1)
    c.setStrokeColor(colors.HexColor("#8a8a8a"))
    c.rect(x, y, size, size, stroke=1, fill=0)


def _draw_detail_header(c: canvas.Canvas, y: float, widths: List[float]):
    x = MARGIN_LEFT
    h = 28
    headers = ["Full Name", "Phone", "Party", "Sex", "Age", "F", "A", "U", "Yard Sign", "MB_Perm"]

    c.setFillColor(COLOR_MAROON)
    c.rect(x, y, sum(widths), h, stroke=0, fill=1)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 12)

    lefts = [x]
    for w in widths[:-1]:
        lefts.append(lefts[-1] + w)

    for idx, header in enumerate(headers):
        lx = lefts[idx]
        w = widths[idx]
        if idx in [0, 1]:
            c.drawString(lx + 10, y + 8, header)
        else:
            c.drawCentredString(lx + w / 2, y + 8, header)

    return y - h - 6


def _draw_street_band(c: canvas.Canvas, y: float, street: str, widths: List[float]):
    h = 24
    c.setFillColor(COLOR_MAROON_DARK)
    c.rect(MARGIN_LEFT, y, sum(widths), h, stroke=0, fill=1)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 13)
    c.drawString(MARGIN_LEFT + 10, y + 6, street[:78])
    return y - h - 5


def _draw_address_row(c: canvas.Canvas, y: float, address: str, widths: List[float]):
    h = 23
    c.setFillColor(COLOR_LIGHT_ROW)
    c.rect(MARGIN_LEFT, y, sum(widths), h, stroke=0, fill=1)
    c.setFillColor(COLOR_TEXT)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(MARGIN_LEFT + 10, y + 6, address[:92])
    return y - h - 2


def _draw_voter_row(c: canvas.Canvas, y: float, row: Dict[str, str], widths: List[float]):
    h = 24
    lefts = [MARGIN_LEFT]
    for w in widths[:-1]:
        lefts.append(lefts[-1] + w)

    c.setStrokeColor(COLOR_TABLE_GRID)
    c.setLineWidth(1)
    c.line(MARGIN_LEFT, y, MARGIN_LEFT + sum(widths), y)

    c.setFillColor(COLOR_TEXT)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(lefts[0] + 14, y - 16 + h, row.get("Full Name", "")[:33])

    c.setFont("Helvetica", 11)
    c.drawString(lefts[1] + 10, y - 16 + h, row.get("Phone", "")[:23])
    c.drawCentredString(lefts[2] + widths[2] / 2, y - 16 + h, row.get("Party", "")[:3])
    c.drawCentredString(lefts[3] + widths[3] / 2, y - 16 + h, row.get("Sex", "")[:1])
    c.drawCentredString(lefts[4] + widths[4] / 2, y - 16 + h, row.get("Age", "")[:3])

    box_y = y + 5
    _checkbox(c, lefts[5] + widths[5] / 2 - 5, box_y)
    _checkbox(c, lefts[6] + widths[6] / 2 - 5, box_y)
    _checkbox(c, lefts[7] + widths[7] / 2 - 5, box_y)
    _checkbox(c, lefts[8] + widths[8] / 2 - 5, box_y)

    if row.get("F"):
        c.drawCentredString(lefts[5] + widths[5] / 2, y - 16 + h, row["F"])
    if row.get("A"):
        c.drawCentredString(lefts[6] + widths[6] / 2, y - 16 + h, row["A"])
    if row.get("U"):
        c.drawCentredString(lefts[7] + widths[7] / 2, y - 16 + h, row["U"])
    if row.get("Yard Sign"):
        c.drawCentredString(lefts[8] + widths[8] / 2, y - 16 + h, row["Yard Sign"])

    c.drawCentredString(lefts[9] + widths[9] / 2, y - 16 + h, row.get("MB_Perm", "")[:3])

    return y - h


def _draw_precinct_pages(c: canvas.Canvas, detail_df: pd.DataFrame, logo_paths: Dict[str, str]):
    if detail_df.empty:
        _draw_page_header(c, "No Precinct Data", logo_paths, show_center_title=True)
        c.setFont("Helvetica", 18)
        c.setFillColor(COLOR_TEXT)
        c.drawCentredString(PAGE_WIDTH / 2, PAGE_HEIGHT / 2, "No records matched the current filters.")
        c.showPage()
        return

    widths = [268, 130, 40, 38, 40, 28, 28, 28, 65, 55]
    detail_df = detail_df.copy()

    for precinct, precinct_df in detail_df.groupby("Precinct", sort=True):
        precinct_rows = precinct_df.to_dict("records")
        row_idx = 0
        continued = False
        bookmark_key = f"precinct_{precinct.replace(' ', '_').replace('/', '_').replace(',', '').replace('-', '_')}"

        while row_idx < len(precinct_rows):
            title = precinct if not continued else f"{precinct} (cont)"
            _draw_page_header(c, title, logo_paths, show_center_title=True)

            if not continued:
                c.bookmarkPage(bookmark_key)
                c.addOutlineEntry(precinct, bookmark_key, level=0, closed=False)

            y = PAGE_HEIGHT - 109
            y = _draw_detail_header(c, y, widths)

            current_street = None
            current_address = None

            while row_idx < len(precinct_rows):
                row = precinct_rows[row_idx]
                street = row.get("Street", "")
                address = row.get("Address", "")

                required = 0
                if street != current_street:
                    required += 29
                if address != current_address:
                    required += 25
                required += 24

                if y < 54 + required:
                    break

                if street != current_street:
                    y = _draw_street_band(c, y, street, widths)
                    current_street = street
                    current_address = None

                if address != current_address:
                    y = _draw_address_row(c, y, address, widths)
                    current_address = address

                y = _draw_voter_row(c, y, row, widths)
                row_idx += 1

            c.showPage()
            continued = True


def generate_door_to_door_pdf(
    filtered: pd.DataFrame,
    created_date: str,
    report_title: str,
    candidate_logo_path: str,
    tss_logo_path: str,
) -> bytes:
    detail_df = build_door_to_door_table(filtered)

    if "Precinct" in detail_df.columns and not detail_df.empty:
        count_records = []
        for precinct, group in detail_df.groupby("Precinct", sort=True):
            count_records.append({
                "Precinct": precinct,
                "Individuals": len(group),
                "Households": group["Address"].nunique(),
            })
        precinct_counts = pd.DataFrame(count_records).sort_values("Precinct").reset_index(drop=True)
    else:
        precinct_counts = pd.DataFrame(columns=["Precinct", "Individuals", "Households"])

    logo_paths = {"candidate_connect": candidate_logo_path, "tss": tss_logo_path}

    buffer = BytesIO()
    c = NumberedCanvas(buffer, pagesize=PAGE_SIZE, created_text=f"Updated: {created_date}", logo_paths=logo_paths)
    _draw_cover_page(c, filtered, report_title, created_date, logo_paths)
    c.showPage()
    _draw_counts_summary_pages(c, precinct_counts, logo_paths)
    _draw_precinct_pages(c, detail_df, logo_paths)
    c.save()
    buffer.seek(0)
    return buffer.read()
