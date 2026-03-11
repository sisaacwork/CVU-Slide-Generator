"""
pptx_gen.py
───────────
Injects query data into the CVU slide template.

Strategy (avoids python-pptx chart limitations):
  1. python-pptx for text placeholder replacement.
  2. zipfile manipulation to update:
       - ppt/charts/chartN.xml   (cached data → charts update instantly)
       - ppt/embeddings/...xlsx  (embedded Excel → editable by users)
  3. Returns PPTX bytes ready to stream as a download.
"""

import io
import copy
import zipfile
import datetime
import logging
from pathlib import Path

from pptx import Presentation
from pptx.util import Pt
import openpyxl
from lxml import etree

log = logging.getLogger(__name__)

TEMPLATE_PATH = Path(__file__).parent / "template.pptx"

# XML namespaces
NS = {
    'c':  'http://schemas.openxmlformats.org/drawingml/2006/chart',
    'a':  'http://schemas.openxmlformats.org/drawingml/2006/main',
    'r':  'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
    'p':  'http://schemas.openxmlformats.org/presentationml/2006',
}

ETREE_NS = {k: v for k, v in NS.items()}


# ─── Text placeholder replacement ───────────────────────────────────────────

def _replace_in_pptx(pptx_bytes: bytes, replacements: dict[str, str]) -> bytes:
    """Use python-pptx to replace {placeholders} in slide text frames."""
    prs = Presentation(io.BytesIO(pptx_bytes))

    for slide in prs.slides:
        for shape in slide.shapes:
            if not shape.has_text_frame:
                continue
            for para in shape.text_frame.paragraphs:
                # Reconstruct full paragraph text to detect split runs
                full = ''.join(run.text for run in para.runs)
                changed = full
                for key, val in replacements.items():
                    changed = changed.replace(key, val)
                if changed != full and para.runs:
                    # Put all replaced text in first run, clear the rest
                    para.runs[0].text = changed
                    for run in para.runs[1:]:
                        run.text = ''

    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


# ─── Chart XML helpers ───────────────────────────────────────────────────────

def _make_str_cache(cats: list[str], ns_c: str) -> etree._Element:
    sc = etree.Element(f'{{{ns_c}}}strCache')
    pc = etree.SubElement(sc, f'{{{ns_c}}}ptCount')
    pc.set('val', str(len(cats)))
    for i, cat in enumerate(cats):
        pt = etree.SubElement(sc, f'{{{ns_c}}}pt')
        pt.set('idx', str(i))
        v = etree.SubElement(pt, f'{{{ns_c}}}v')
        v.text = str(cat)
    return sc


def _make_num_cache(vals: list, fmt: str, ns_c: str) -> etree._Element:
    nc = etree.Element(f'{{{ns_c}}}numCache')
    fc = etree.SubElement(nc, f'{{{ns_c}}}formatCode')
    fc.text = fmt
    pc = etree.SubElement(nc, f'{{{ns_c}}}ptCount')
    non_none = [(i, v) for i, v in enumerate(vals) if v is not None]
    pc.set('val', str(len(non_none)))
    for i, v in non_none:
        pt = etree.SubElement(nc, f'{{{ns_c}}}pt')
        pt.set('idx', str(i))
        ve = etree.SubElement(pt, f'{{{ns_c}}}v')
        ve.text = str(v)
    return nc


def _update_formula_range(formula: str, new_count: int) -> str:
    """Update Sheet1!$A$2:$A$52 → Sheet1!$A$2:$A${new_count+1}"""
    import re
    def repl(m):
        return f"{m.group(1)}{new_count + 1}"
    return re.sub(r'(\$[A-Z]+\$)\d+$', repl, formula)


def _update_chart_series(chart_xml: bytes, series_list: list[dict]) -> bytes:
    """
    series_list items:
      { idx:int, cats:[str,...], vals:[num|None,...], val_fmt:'General' }
    """
    ns_c = NS['c']
    root = etree.fromstring(chart_xml)

    all_sers = root.findall(f'.//{{{ns_c}}}ser')

    for spec in series_list:
        idx = spec['idx']
        if idx >= len(all_sers):
            continue
        ser = all_sers[idx]
        count = len(spec.get('cats', spec.get('vals', [])))

        # Update categories (strRef)
        if 'cats' in spec:
            cats = spec['cats']
            str_ref = ser.find(f'.//{{{ns_c}}}strRef')
            if str_ref is not None:
                f_el = str_ref.find(f'{{{ns_c}}}f')
                if f_el is not None:
                    f_el.text = _update_formula_range(f_el.text or '', len(cats))
                old_sc = str_ref.find(f'{{{ns_c}}}strCache')
                if old_sc is not None:
                    str_ref.remove(old_sc)
                str_ref.append(_make_str_cache(cats, ns_c))

        # Update values (numRef)
        if 'vals' in spec:
            vals = spec['vals']
            fmt  = spec.get('val_fmt', 'General')
            num_ref = ser.find(f'.//{{{ns_c}}}numRef')
            if num_ref is not None:
                f_el = num_ref.find(f'{{{ns_c}}}f')
                if f_el is not None:
                    f_el.text = _update_formula_range(f_el.text or '', count)
                old_nc = num_ref.find(f'{{{ns_c}}}numCache')
                if old_nc is not None:
                    num_ref.remove(old_nc)
                num_ref.append(_make_num_cache(vals, fmt, ns_c))

    return etree.tostring(root, xml_declaration=True, encoding='UTF-8')


# ─── Excel helpers ───────────────────────────────────────────────────────────

def _update_excel_single_series(xlsx_bytes: bytes, headers: list[str],
                                 rows: list[tuple]) -> bytes:
    """Rewrite Sheet1 with headers in row 1 and data rows below."""
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes))
    ws = wb.active
    ws.delete_rows(1, ws.max_row + 1)
    ws.append(headers)
    for row in rows:
        ws.append(list(row))
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ─── Per-chart update ────────────────────────────────────────────────────────

def _chart1_updates(s2: dict):
    """Slide 2: 50-year cumulative growth (1 series)."""
    years  = s2['years']
    vals   = s2['values']
    series = [{'idx': 0, 'cats': years, 'vals': vals}]
    xl_rows = list(zip(years, vals))
    return series, ['Year', 'Total Buildings'], xl_rows


def _chart2_updates(s3: dict):
    """Slide 3: City vs other cities (2 series)."""
    years = s3['years']
    cv    = s3['city_values']
    ov    = s3['other_values']
    series = [
        {'idx': 0, 'cats': years, 'vals': cv},
        {'idx': 1, 'cats': years, 'vals': ov},
    ]
    xl_rows = list(zip(years, cv, ov))
    return series, ['Year', 'City', 'All Other Cities'], xl_rows


def _chart3_updates(s4: dict):
    """Slide 4: Projected growth (2 series — historical / projected)."""
    years = s4['years']
    hv    = s4['hist_values']
    pv    = s4['proj_values']
    series = [
        {'idx': 0, 'cats': years, 'vals': hv},
        {'idx': 1, 'cats': years, 'vals': pv},
    ]
    xl_rows = list(zip(years, hv, pv))
    return series, ['Year', 'Total Buildings', 'Projected'], xl_rows


def _chart4_updates(s5: dict):
    """Slide 5: Buildings + population (4 series)."""
    years = s5['years']
    series = [
        {'idx': 0, 'cats': years, 'vals': s5['hist_builds']},
        {'idx': 1, 'cats': years, 'vals': s5['proj_builds']},
        {'idx': 2, 'cats': years, 'vals': s5['hist_pop'],   'val_fmt': '#,##0'},
        {'idx': 3, 'cats': years, 'vals': s5['future_pop'], 'val_fmt': '#,##0'},
    ]
    xl_rows = list(zip(years, s5['hist_builds'], s5['proj_builds'],
                       s5['hist_pop'], s5['future_pop']))
    return series, ['Year', 'Total Buildings', 'Future Buildings',
                    'Total Population', 'Future Population'], xl_rows


def _chart5_updates(s6: dict):
    """Slide 6: Function pie."""
    cats = s6['func_categories']
    vals = s6['func_values']
    series = [{'idx': 0, 'cats': cats, 'vals': vals}]
    xl_rows = list(zip(cats, vals))
    return series, [' ', 'Building Function'], xl_rows


def _chart6_updates(s6: dict):
    """Slide 6: Material pie."""
    cats = s6['mat_categories']
    vals = s6['mat_values']
    series = [{'idx': 0, 'cats': cats, 'vals': vals}]
    xl_rows = list(zip(cats, vals))
    return series, [' ', 'Structural Material'], xl_rows


# ─── Main entry point ────────────────────────────────────────────────────────

def generate_pptx(
    city_name:    str,
    country_name: str,
    threshold:    int,
    slide_data:   dict,        # keys: 's2', 's3', 's4', 's5', 's6'
    selected_slides: list[int] | None = None,  # None = all
) -> bytes:
    """
    Build and return PPTX bytes.

    slide_data keys:
      s2 → slide2_data output
      s3 → slide3_data output (may be None for agglomerations)
      s4 → slide4_data output
      s5 → slide5_data output
      s6 → slide6_data output
    """
    today = datetime.date.today()
    month_abbr = today.strftime('%b').upper()

    replacements = {
        '{city}':            city_name,
        '{country}':         country_name,
        '{threshold}':       str(threshold),
        '{year}':            str(today.year),
        '{month}':           month_abbr,
        '{day}':             f"{today.day:02d}",
        '{2000_growth_pct}': str(slide_data.get('s2', {}).get('growth_pct_2000', 0)),
    }

    # Step 1: text replacement via python-pptx
    template_bytes = TEMPLATE_PATH.read_bytes()
    pptx_bytes = _replace_in_pptx(template_bytes, replacements)

    # Step 2: chart data injection via zipfile manipulation
    in_zip  = zipfile.ZipFile(io.BytesIO(pptx_bytes))
    out_buf = io.BytesIO()

    # Pre-compute chart updates (map chart filename → series + excel update)
    chart_series: dict[str, list[dict]] = {}
    chart_excel:  dict[str, tuple] = {}   # chart_file → (headers, rows)

    s2 = slide_data.get('s2')
    s3 = slide_data.get('s3')
    s4 = slide_data.get('s4')
    s5 = slide_data.get('s5')
    s6 = slide_data.get('s6')

    if s2:
        ser, hdrs, rows = _chart1_updates(s2)
        chart_series['chart1.xml'] = ser
        chart_excel['chart1.xml'] = (hdrs, rows, 'Microsoft_Excel_Worksheet.xlsx')

    if s3:
        ser, hdrs, rows = _chart2_updates(s3)
        chart_series['chart2.xml'] = ser
        chart_excel['chart2.xml'] = (hdrs, rows, 'Microsoft_Excel_Worksheet1.xlsx')

    if s4:
        ser, hdrs, rows = _chart3_updates(s4)
        chart_series['chart3.xml'] = ser
        chart_excel['chart3.xml'] = (hdrs, rows, 'Microsoft_Excel_Worksheet2.xlsx')

    if s5:
        ser, hdrs, rows = _chart4_updates(s5)
        chart_series['chart4.xml'] = ser
        chart_excel['chart4.xml'] = (hdrs, rows, 'Microsoft_Excel_Worksheet3.xlsx')

    if s6:
        ser5, hdrs5, rows5 = _chart5_updates(s6)
        chart_series['chart5.xml'] = ser5
        chart_excel['chart5.xml'] = (hdrs5, rows5, 'Microsoft_Excel_Worksheet4.xlsx')

        ser6, hdrs6, rows6 = _chart6_updates(s6)
        chart_series['chart6.xml'] = ser6
        chart_excel['chart6.xml'] = (hdrs6, rows6, 'Microsoft_Excel_Worksheet5.xlsx')

    # Build reverse map: excel filename → updated bytes
    excel_updates: dict[str, bytes] = {}
    for chart_name, (hdrs, rows, xl_fname) in chart_excel.items():
        if xl_fname in excel_updates:
            continue
        try:
            xl_path = f'ppt/embeddings/{xl_fname}'
            xl_bytes = in_zip.read(xl_path)
            xl_bytes = _update_excel_single_series(xl_bytes, hdrs, rows)
            excel_updates[xl_fname] = xl_bytes
        except Exception as e:
            log.warning("Excel update failed for %s: %s", xl_fname, e)

    with zipfile.ZipFile(out_buf, 'w', zipfile.ZIP_DEFLATED) as out_zip:
        for item in in_zip.infolist():
            content = in_zip.read(item.filename)
            fname   = item.filename

            # Update chart XML caches
            if 'ppt/charts/' in fname and fname.endswith('.xml'):
                chart_basename = Path(fname).name
                if chart_basename in chart_series:
                    try:
                        content = _update_chart_series(content, chart_series[chart_basename])
                    except Exception as e:
                        log.warning("Chart XML update failed for %s: %s", fname, e)

            # Update embedded Excel
            elif 'ppt/embeddings/' in fname and fname.endswith('.xlsx'):
                xl_basename = Path(fname).name
                if xl_basename in excel_updates:
                    content = excel_updates[xl_basename]

            out_zip.writestr(item, content)

    in_zip.close()
    return out_buf.getvalue()
