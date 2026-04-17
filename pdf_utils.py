from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
import re
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

PAGE_SIZE = landscape(LETTER)
PAGE_W, PAGE_H = PAGE_SIZE
LEFT = 26
RIGHT = PAGE_W - 26
TOP = PAGE_H - 18
BOTTOM = 22
HEADER_H = 52
FOOTER_H = 22
CONTENT_TOP = TOP - HEADER_H - 8
CONTENT_BOTTOM = BOTTOM + FOOTER_H + 8

BRAND_RED = colors.HexColor('#7a1523')
BRAND_GRAY = colors.HexColor('#4b4f54')
DARK = colors.HexColor('#2f3134')
MID = colors.HexColor('#696c70')
LIGHT = colors.HexColor('#faf9f9')
LIGHT_RED = colors.HexColor('#f3eded')
STREET_FILL = colors.HexColor('#5c0f1b')
HOUSE_FILL = colors.HexColor('#ece8e8')
ALT_FILL = colors.HexColor('#f7f5f5')
GRID = colors.HexColor('#cfc9c9')

TITLE_WORD_EXCEPTIONS = {
    'GOP': 'GOP', 'MDJ': 'MDJ', 'USC': 'USC', 'STS': 'STS', 'STH': 'STH',
    'PA': 'PA', 'RD': 'Rd', 'ST': 'St', 'AVE': 'Ave', 'BLVD': 'Blvd', 'DR': 'Dr',
    'LN': 'Ln', 'PL': 'Pl', 'CT': 'Ct', 'CIR': 'Cir', 'PKWY': 'Pkwy', 'HWY': 'Hwy',
    'APT': 'Apt', 'N': 'N', 'S': 'S', 'E': 'E', 'W': 'W', 'NE': 'NE', 'NW': 'NW',
    'SE': 'SE', 'SW': 'SW', 'II': 'II', 'III': 'III', 'IV': 'IV', 'JR': 'Jr', 'SR': 'Sr'
}


@dataclass
class PageSpec:
    kind: str
    data: object


def _safe_text_series(series: pd.Series, blank: str = '') -> pd.Series:
    series = pd.Series(series, index=getattr(series, 'index', None), copy=False)
    series = series.astype('object')
    series = series.where(series.notna(), blank)
    out = series.astype(str)
    return out.replace({'nan': blank, 'None': blank})


def _text(val) -> str:
    if pd.isna(val):
        return ''
    s = str(val).strip()
    return '' if s.lower() == 'nan' else s


def _smart_title(text: object) -> str:
    t = _text(text)
    if not t:
        return ''
    words = []
    for word in t.replace('_', ' ').split():
        upper = word.upper()
        words.append(TITLE_WORD_EXCEPTIONS.get(upper, word.capitalize()))
    return ' '.join(words)


def _display_title(text: str) -> str:
    return _smart_title(text)


def _bool_yn(val) -> str:
    s = _text(val).lower()
    if s in {'true', 't', '1', 'yes', 'y'}:
        return 'Y'
    if s in {'false', 'f', '0', 'no', 'n'}:
        return 'N'
    return ''


def _phone_digits(val) -> str:
    raw = _text(val)
    if not raw:
        return ''
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) >= 10:
        return digits[-10:]
    return ''

def _phone(val) -> str:
    digits = _phone_digits(val)
    if len(digits) == 10:
        return f'({digits[:3]}) {digits[3:6]}-{digits[6:]}'
    return ''

def _preferred_phone_label(row: pd.Series) -> str:
    mobile = _phone(row.get('Mobile', ''))
    if mobile:
        return f'{mobile} (m)'
    landline = _phone(row.get('Landline', ''))
    if landline:
        return f'{landline} (l)'
    return ''


def _num_sort(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.extract(r'(\d+)')[0], errors='coerce').fillna(0)


def _full_name(df: pd.DataFrame) -> pd.Series:
    if 'FullName' in df.columns:
        full = _safe_text_series(df['FullName']).map(_smart_title)
        if not full.eq('').all():
            return full
    first = _safe_text_series(df['FirstName']) if 'FirstName' in df.columns else pd.Series([''] * len(df), index=df.index)
    middle = _safe_text_series(df['MiddleName']) if 'MiddleName' in df.columns else pd.Series([''] * len(df), index=df.index)
    last = _safe_text_series(df['LastName']) if 'LastName' in df.columns else pd.Series([''] * len(df), index=df.index)
    suffix = _safe_text_series(df['NameSuffix']) if 'NameSuffix' in df.columns else pd.Series([''] * len(df), index=df.index)
    out = first.str.strip() + ' ' + middle.str.strip() + ' ' + last.str.strip() + ' ' + suffix.str.strip()
    return out.str.replace(r'\s+', ' ', regex=True).str.strip().map(_smart_title)


def _household_key(df: pd.DataFrame) -> pd.Series:
    house = _safe_text_series(df['House Number']) if 'House Number' in df.columns else pd.Series([''] * len(df), index=df.index)
    street = _safe_text_series(df['Street Name']) if 'Street Name' in df.columns else pd.Series([''] * len(df), index=df.index)
    apt = _safe_text_series(df['Apartment Number']) if 'Apartment Number' in df.columns else pd.Series([''] * len(df), index=df.index)
    fallback = house + '|' + street + '|' + apt
    if 'HH_ID' in df.columns:
        hh = df['HH_ID'].astype('object').astype(str).str.strip()
        if (hh != '').any():
            return hh.where(hh != '', fallback)
    return fallback


def prepare_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    status_col = 'VoterStatus' if 'VoterStatus' in out.columns else ('voterstatus' if 'voterstatus' in out.columns else None)
    if status_col:
        out = out[out[status_col].astype(str).str.strip().str.upper() == 'A'].copy()
    for col in ['Precinct', 'Street Name', 'House Number', 'Apartment Number', 'PrimaryPhone', 'Landline', 'Mobile', 'Phone', 'Party', 'Gender', 'Age', 'MB_Perm']:
        if col not in out.columns:
            out[col] = ''
        else:
            out[col] = out[col].astype('object')
    out['_full_name'] = _full_name(out)
    out['_phone_fmt'] = out.apply(_preferred_phone_label, axis=1)
    out['_mb_fmt'] = out['MB_Perm'].map(_bool_yn)
    out['_hh_key'] = _household_key(out)
    out['_house_no_sort'] = _num_sort(out['House Number'])
    out['_apt_sort'] = _num_sort(out['Apartment Number'])
    out['Precinct'] = _safe_text_series(out['Precinct'], '(Blank)').map(_smart_title)
    out['Street Name'] = _safe_text_series(out['Street Name']).map(_smart_title)
    out['House Number'] = _safe_text_series(out['House Number'])
    out['Apartment Number'] = _safe_text_series(out['Apartment Number']).map(_smart_title)
    return out.sort_values(['Precinct', 'Street Name', '_house_no_sort', 'House Number', '_apt_sort', 'Apartment Number', '_full_name'])


def _summary_df(prepared: pd.DataFrame) -> pd.DataFrame:
    return (
        prepared.groupby('Precinct', dropna=False)
        .agg(Individuals=('Precinct', 'size'), Households=('_hh_key', 'nunique'))
        .reset_index()
        .sort_values('Precinct')
    )


def _largest_area(selected_filters: dict[str, list[str]], prepared: pd.DataFrame) -> str:
    for key in ['County', 'STH', 'STS', 'USC', 'School District', 'Municipality', 'Ward', 'Precinct']:
        vals = selected_filters.get(key, []) if selected_filters else []
        if vals:
            return _display_title(vals[0]) if len(vals) == 1 else f'{len(vals)} {key} selections'
    for key in ['County', 'Municipality', 'Precinct']:
        if key in prepared.columns and prepared[key].nunique(dropna=True) == 1:
            return _display_title(prepared[key].dropna().astype(str).iloc[0])
    return 'Selected Area'


def _party_desc(selected_filters: dict[str, list[str]]) -> str:
    party = selected_filters.get('Party', []) if selected_filters else []
    if not party:
        return 'Selected voters'
    if len(party) == 1:
        mapping = {'R': 'GOP voters', 'D': 'Democratic voters', 'I': 'Independent voters'}
        return mapping.get(str(party[0]).upper(), f'{party[0]} voters')
    return 'Selected voters'


def _criteria_sentence(selected_filters: dict[str, list[str]], prepared: pd.DataFrame) -> str:
    party_desc = _party_desc(selected_filters)
    extras = []
    for key in ['Municipality', 'Precinct', 'Ward', 'County', 'School District', 'USC', 'STS', 'STH', 'MDJ']:
        vals = selected_filters.get(key, []) if selected_filters else []
        if vals:
            if len(vals) == 1:
                extras.append(_display_title(vals[0]))
            else:
                extras.append(f'{len(vals)} {key.lower()} selections')
    if extras:
        return f'{party_desc} in ' + ', '.join(extras[:3])
    return f'{party_desc} in {_display_title(_largest_area(selected_filters, prepared))}'


def _load_logos(base_dir: Path):
    tss = cc = None
    tss_path = base_dir / 'TSS_Logo_Transparent.png'
    cc_path = base_dir / 'candidate_connect_logo.png'
    if tss_path.exists():
        tss = ImageReader(str(tss_path))
    if cc_path.exists():
        cc = ImageReader(str(cc_path))
    return tss, cc


def _draw_header(c: canvas.Canvas, page_num: int, total_pages: int, tss_logo, cc_logo, precinct_title: str | None = None, continued: bool = False):
    c.setFillColor(colors.white)
    c.rect(0, PAGE_H - HEADER_H - 14, PAGE_W, HEADER_H + 14, stroke=0, fill=1)
    c.setStrokeColor(BRAND_GRAY)
    c.setLineWidth(1)
    c.line(LEFT, PAGE_H - HEADER_H - 10, RIGHT, PAGE_H - HEADER_H - 10)
    if cc_logo:
        c.drawImage(cc_logo, LEFT, PAGE_H - 45, width=148, height=30, preserveAspectRatio=True, mask='auto')
    if tss_logo:
        c.setFillColor(MID)
        c.setFont('Helvetica-Bold', 8)
        powered_x = RIGHT - 54
        c.drawCentredString(powered_x, PAGE_H - 18, 'Powered By')
        c.drawImage(tss_logo, powered_x - 37, PAGE_H - 42, width=74, height=20, preserveAspectRatio=True, mask='auto')
    if precinct_title:
        title = _text(precinct_title) + (' (cont)' if continued else '')
        c.setFillColor(DARK)
        c.setFont('Helvetica-Bold', 13)
        c.drawCentredString(PAGE_W / 2, PAGE_H - 31, title)
    c.setFont('Helvetica', 8)
    c.setFillColor(MID)
    c.drawString(LEFT, 10, f'{page_num} of {total_pages}')
    c.drawRightString(RIGHT, 10, f'Updated: {datetime.now().strftime("%m/%d/%Y")}')


def _draw_cover(c: canvas.Canvas, prepared: pd.DataFrame, selected_filters: dict[str, list[str]], tss_logo, cc_logo, page_num: int, total_pages: int):
    _draw_header(c, page_num, total_pages, tss_logo, cc_logo)
    area = _display_title(_largest_area(selected_filters, prepared))
    desc = _criteria_sentence(selected_filters, prepared)

    if cc_logo:
        c.drawImage(cc_logo, PAGE_W / 2 - 165, PAGE_H / 2 + 82, width=330, height=82, preserveAspectRatio=True, mask='auto')
    c.setFillColor(DARK)
    c.setFont('Helvetica-Bold', 24)
    c.drawCentredString(PAGE_W / 2, PAGE_H / 2 + 18, area)
    c.setFont('Helvetica', 14)
    c.drawCentredString(PAGE_W / 2, PAGE_H / 2 - 10, datetime.now().strftime('%B %d, %Y'))
    c.setFont('Helvetica', 12)
    c.drawCentredString(PAGE_W / 2, PAGE_H / 2 - 38, desc)

    c.setFillColor(LIGHT_RED)
    c.roundRect(PAGE_W / 2 - 225, PAGE_H / 2 - 120, 450, 62, 10, stroke=0, fill=1)
    c.setFillColor(BRAND_RED)
    c.setFont('Helvetica-Bold', 13)
    c.drawCentredString(PAGE_W / 2, PAGE_H / 2 - 87, f'Individuals: {len(prepared):,}     Households: {prepared["_hh_key"].nunique():,}')


def _draw_summary_page(c: canvas.Canvas, chunk: pd.DataFrame, max_individuals: int, tss_logo, cc_logo, page_num: int, total_pages: int):
    _draw_header(c, page_num, total_pages, tss_logo, cc_logo, precinct_title='Precinct Counts Summary')
    y = CONTENT_TOP
    c.setFillColor(DARK)
    c.setFont('Helvetica-Bold', 15)
    c.drawString(LEFT, y, 'Precinct Counts Summary')
    y -= 24

    total_w = RIGHT - LEFT
    col_w = [total_w - 250, 120, 130]
    col_x = [LEFT, LEFT + col_w[0], LEFT + col_w[0] + col_w[1]]
    row_h = 20

    c.setFillColor(BRAND_RED)
    c.rect(LEFT, y - row_h + 4, total_w, row_h, stroke=0, fill=1)
    c.setFillColor(colors.white)
    c.setFont('Helvetica-Bold', 10)
    c.drawString(col_x[0] + 8, y - 10, 'Precinct')
    c.drawCentredString(col_x[1] + col_w[1] / 2, y - 10, 'Individuals')
    c.drawCentredString(col_x[2] + col_w[2] / 2, y - 10, 'Households')
    y -= row_h

    c.setFont('Helvetica', 10)
    for _, row in chunk.iterrows():
        ratio = 0 if max_individuals == 0 else float(row['Individuals']) / max_individuals
        fill = colors.Color(0.97, max(0.88, 0.97 - ratio * 0.11), max(0.88, 0.97 - ratio * 0.11))
        c.setFillColor(fill)
        c.rect(LEFT, y - row_h + 4, total_w, row_h, stroke=0, fill=1)
        c.setStrokeColor(GRID)
        c.rect(LEFT, y - row_h + 4, total_w, row_h, stroke=1, fill=0)
        c.line(col_x[1], y - row_h + 4, col_x[1], y + 4)
        c.line(col_x[2], y - row_h + 4, col_x[2], y + 4)
        c.setFillColor(DARK)
        c.drawString(col_x[0] + 8, y - 10, str(row['Precinct']))
        c.drawCentredString(col_x[1] + col_w[1] / 2, y - 10, f"{int(row['Individuals']):,}")
        c.drawCentredString(col_x[2] + col_w[2] / 2, y - 10, f"{int(row['Households']):,}")
        y -= row_h


def _measure_precinct_pages(prepared: pd.DataFrame):
    pages = []
    for precinct, precinct_df in prepared.groupby('Precinct', dropna=False, sort=False):
        precinct_name = _text(precinct) or '(Blank)'
        chunks = []
        chunk = []
        y = CONTENT_TOP - 36
        current_street = None
        for _, hh_df in precinct_df.groupby('_hh_key', sort=False):
            first = hh_df.iloc[0]
            street = _text(first['Street Name'])
            address = _text(first['House Number'])
            if _text(first.get('Apartment Number', '')):
                address += f"  Apt {_text(first['Apartment Number'])}"
            address += f"  {street}"
            needed = 0
            if street != current_street:
                needed += 24
            needed += 18 + (len(hh_df) * 17) + 5
            if y - needed < CONTENT_BOTTOM and chunk:
                chunks.append(chunk)
                chunk = []
                y = CONTENT_TOP - 36
                current_street = None
            if street != current_street:
                chunk.append({'type': 'street', 'text': street})
                y -= 24
                current_street = street
            chunk.append({'type': 'household', 'text': address})
            y -= 18
            for _, person in hh_df.iterrows():
                chunk.append({'type': 'voter', 'data': person.to_dict()})
                y -= 17
            y -= 5
        if chunk:
            chunks.append(chunk)
        for i, ch in enumerate(chunks):
            pages.append({'precinct': precinct_name, 'continued': i > 0, 'rows': ch})
    return pages


def _draw_list_page(c: canvas.Canvas, page: dict, tss_logo, cc_logo, page_num: int, total_pages: int):
    _draw_header(c, page_num, total_pages, tss_logo, cc_logo, precinct_title=page['precinct'], continued=page['continued'])
    y = CONTENT_TOP
    headers = [
        ('Full Name', LEFT + 12), ('Phone', LEFT + 280), ('Party', LEFT + 386), ('Sex', LEFT + 424),
        ('Age', LEFT + 452), ('F', LEFT + 488), ('A', LEFT + 516), ('U', LEFT + 544), ('Yard Sign', LEFT + 570), ('MB_Perm', LEFT + 635),
    ]
    c.setFillColor(BRAND_RED)
    c.rect(LEFT, y - 18, RIGHT - LEFT, 18, stroke=0, fill=1)
    c.setFillColor(colors.white)
    c.setFont('Helvetica-Bold', 8)
    for text, x in headers:
        c.drawString(x, y - 12, text)
    y -= 22

    alt = False
    for row in page['rows']:
        if row['type'] == 'street':
            c.setFillColor(STREET_FILL)
            c.rect(LEFT, y - 17, RIGHT - LEFT, 17, stroke=0, fill=1)
            c.setFillColor(colors.white)
            c.setFont('Helvetica-Bold', 9)
            c.drawString(LEFT + 8, y - 12, row['text'][:95])
            y -= 21
        elif row['type'] == 'household':
            c.setFillColor(HOUSE_FILL)
            c.rect(LEFT, y - 14, RIGHT - LEFT, 14, stroke=0, fill=1)
            c.setFillColor(DARK)
            c.setFont('Helvetica-Bold', 8.5)
            c.drawString(LEFT + 12, y - 10, row['text'][:110])
            y -= 16
        else:
            person = row['data']
            if alt:
                c.setFillColor(ALT_FILL)
                c.rect(LEFT, y - 13, RIGHT - LEFT, 13, stroke=0, fill=1)
            alt = not alt
            c.setFillColor(DARK)
            c.setFont('Helvetica-Bold', 8)
            c.drawString(LEFT + 15, y - 9, _text(person.get('_full_name', ''))[:40])
            c.setFont('Helvetica', 8)
            c.drawString(LEFT + 280, y - 9, _text(person.get('_phone_fmt', '')))
            c.drawString(LEFT + 388, y - 9, _text(person.get('Party', '')))
            c.drawString(LEFT + 426, y - 9, _text(person.get('Gender', '')))
            c.drawRightString(LEFT + 468, y - 9, _text(person.get('Age', '')))
            for x in [LEFT + 491, LEFT + 519, LEFT + 547, LEFT + 588]:
                c.rect(x, y - 11, 8, 8, stroke=1, fill=0)
            c.drawString(LEFT + 639, y - 9, _text(person.get('_mb_fmt', '')))
            c.setStrokeColor(GRID)
            c.line(LEFT, y - 13, RIGHT, y - 13)
            y -= 17


def generate_walk_list_pdf(df: pd.DataFrame, report_title: str, report_description: str, selected_filters: dict[str, list[str]], base_dir: Path) -> bytes:
    prepared = prepare_report_dataframe(df)
    tss_logo, cc_logo = _load_logos(base_dir)
    summary = _summary_df(prepared)
    max_individuals = int(summary['Individuals'].max()) if not summary.empty else 0
    summary_pages = [summary.iloc[i:i + 20] for i in range(0, len(summary), 20)] or [pd.DataFrame(columns=summary.columns)]
    list_pages = _measure_precinct_pages(prepared)
    pages = [PageSpec('cover', None)] + [PageSpec('summary', s) for s in summary_pages] + [PageSpec('list', p) for p in list_pages]

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=PAGE_SIZE)
    total_pages = len(pages)
    outlined_precincts = set()

    for idx, page in enumerate(pages, start=1):
        if page.kind == 'cover':
            c.bookmarkPage('cover')
            c.addOutlineEntry('Cover', 'cover', level=0, closed=False)
            _draw_cover(c, prepared, selected_filters, tss_logo, cc_logo, idx, total_pages)
        elif page.kind == 'summary':
            key = f'summary_{idx}'
            c.bookmarkPage(key)
            if idx == 2:
                c.addOutlineEntry('Precinct Counts Summary', key, level=0, closed=False)
            _draw_summary_page(c, page.data, max_individuals, tss_logo, cc_logo, idx, total_pages)
        else:
            precinct_key = re.sub(r'[^A-Za-z0-9_]+', '_', page.data['precinct']) or f'precinct_{idx}'
            if precinct_key not in outlined_precincts:
                c.bookmarkPage(precinct_key)
                c.addOutlineEntry(page.data['precinct'], precinct_key, level=0, closed=False)
                outlined_precincts.add(precinct_key)
            _draw_list_page(c, page.data, tss_logo, cc_logo, idx, total_pages)
        c.showPage()
    c.save()
    return buffer.getvalue()
