"""
queries.py — all SQL data retrieval for each slide.

Geography types: city | agglomeration | country | region
Each slide function accepts geo_id + geo_type and builds
the appropriate WHERE clause via _geo_filter().
"""
import logging
import datetime
import re

from db import cvu_conn, ghsl_conn

log = logging.getLogger(__name__)

START_YEAR = 1975
COMPLETED_STATUSES = ('COM',)

# ─── Column constants ────────────────────────────────────────────────────────
HEIGHT_COL = 'height_architecture'
YEAR_COL   = 'completed'          # integer column
CITY_COL   = 'city_id'
STATUS_COL = 'status'
MAT_COL    = 'structural_material'

# Slide 6 — Function: 5 fixed buckets
# main_use_01/02 are enums so must cast to ::text before comparisons
FUNC_EXPR = """
CASE
  WHEN main_use_02::text IS NOT NULL AND main_use_02::text != ''
    THEN 'Mixed-use'
  WHEN LOWER(main_use_01::text) LIKE '%%office%%'
    THEN 'All-Office'
  WHEN LOWER(main_use_01::text) LIKE '%%residential%%'
    THEN 'All-Residential'
  WHEN LOWER(main_use_01::text) LIKE '%%hotel%%'
    THEN 'All-Hotel'
  ELSE 'Other'
END
""".strip()

# Slide 6 — Material: 5 fixed buckets
# composite checked first as it may contain 'concrete'/'steel' in its name
MAT_EXPR = """
CASE
  WHEN LOWER(structural_material::text) LIKE '%%composite%%'
    THEN 'Composite'
  WHEN LOWER(structural_material::text) LIKE '%%concrete%%'
   AND LOWER(structural_material::text) LIKE '%%steel%%'
    THEN 'Concrete-Steel Hybrid'
  WHEN LOWER(structural_material::text) LIKE '%%concrete%%'
    THEN 'All-Concrete'
  WHEN LOWER(structural_material::text) LIKE '%%steel%%'
    THEN 'All-Steel'
  ELSE 'Other'
END
""".strip()


# ─── Geography filter helper ─────────────────────────────────────────────────

def _geo_filter(geo_type: str, param: str = 'geo_id') -> str:
    """Return a SQL WHERE fragment for the given geography type."""
    if geo_type == 'city':
        return f"b.{CITY_COL} = %({param})s"
    elif geo_type == 'agglomeration':
        return f"""b.{CITY_COL} IN (
            SELECT c.id FROM v2_cities c
            JOIN agglomeration_countries ac ON ac.country_id = c.country_id
            WHERE ac.agglomeration_id = %({param})s)"""
    elif geo_type == 'country':
        return f"b.country_id = %({param})s"
    elif geo_type == 'region':
        return f"b.region_id = %({param})s"
    else:
        raise ValueError(f"Unknown geo_type: {geo_type!r}")


def _make_cumulative(yearly_dict, year_range):
    result = []
    running = 0
    for y in year_range:
        running += yearly_dict.get(y, 0)
        result.append((y, running))
    return result


# ─── City / country / region lists ──────────────────────────────────────────

def get_city_list():
    """Return merged list of cities, agglomerations, countries and regions."""
    with cvu_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT c.id, c.name, co.name AS country_name, co.id AS country_id
            FROM v2_cities c
            JOIN v2_countries co ON co.id = c.country_id
            ORDER BY c.name
        """)
        cities = [
            {"type": "city", "id": r[0], "name": r[1],
             "country": r[2], "country_id": r[3]}
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT a.id, a.name_intl AS name
            FROM agglomerations a
            ORDER BY a.name_intl
        """)
        agglos = [
            {"type": "agglomeration", "id": r[0], "name": r[1],
             "country": None, "country_id": None}
            for r in cur.fetchall()
        ]

        cur.execute("SELECT id, name FROM v2_countries ORDER BY name")
        countries = [
            {"type": "country", "id": r[0], "name": r[1],
             "country": None, "country_id": r[0]}
            for r in cur.fetchall()
        ]

        cur.execute("SELECT id, name FROM v2_regions ORDER BY name")
        regions = [
            {"type": "region", "id": r[0], "name": r[1],
             "country": None, "country_id": None}
            for r in cur.fetchall()
        ]

    return cities + agglos + countries + regions


# ─── Slide 2: 50 Years of Cumulative Growth ─────────────────────────────────

def slide2_data(geo_id: int, geo_type: str, threshold: int):
    current_year = datetime.date.today().year
    city_filter  = _geo_filter(geo_type)

    sql = f"""
        SELECT {YEAR_COL} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE {city_filter}
          AND CAST({HEIGHT_COL} AS numeric) >= %(threshold)s
          AND {STATUS_COL} IN %(statuses)s
          AND {YEAR_COL} IS NOT NULL
          AND {YEAR_COL} BETWEEN %(start)s AND %(end_yr)s
        GROUP BY yr ORDER BY yr
    """

    with cvu_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, {
            'geo_id': geo_id, 'threshold': threshold,
            'statuses': COMPLETED_STATUSES,
            'start': START_YEAR, 'end_yr': current_year,
        })
        rows = dict(cur.fetchall())

    year_range  = list(range(START_YEAR, current_year + 1))
    cumulative  = _make_cumulative(rows, year_range)
    years_labels = [str(y) if y < current_year else f"{y}*" for y in year_range]
    values       = [c for _, c in cumulative]

    val_2000   = next((c for y, c in cumulative if y == 2000), 0)
    val_now    = values[-1] if values else 0
    growth_pct = round((val_now - val_2000) / val_2000 * 100) if val_2000 else 0

    return {'years': years_labels, 'values': values, 'growth_pct_2000': growth_pct}


# ─── Slide 3: City vs Other Cities in Country (cities only) ─────────────────

def slide3_data(geo_id: int, geo_type: str, country_id: int, threshold: int):
    """Only meaningful for city geo_type. Returns None otherwise."""
    if geo_type != 'city':
        return None

    current_year = datetime.date.today().year

    base = f"""
        SELECT {YEAR_COL} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE CAST({HEIGHT_COL} AS numeric) >= %(threshold)s
          AND {STATUS_COL} IN %(statuses)s
          AND {YEAR_COL} IS NOT NULL
          AND {YEAR_COL} BETWEEN %(start)s AND %(end_yr)s
    """
    params = {
        'geo_id': geo_id, 'country_id': country_id,
        'threshold': threshold, 'statuses': COMPLETED_STATUSES,
        'start': START_YEAR, 'end_yr': current_year,
    }

    with cvu_conn() as conn:
        cur = conn.cursor()
        cur.execute(base + f" AND b.{CITY_COL} = %(geo_id)s GROUP BY yr ORDER BY yr", params)
        city_rows = dict(cur.fetchall())

        cur.execute(base + f"""
            AND b.{CITY_COL} IN (
                SELECT id FROM v2_cities
                WHERE country_id = %(country_id)s AND id != %(geo_id)s
            )
            GROUP BY yr ORDER BY yr
        """, params)
        other_rows = dict(cur.fetchall())

    year_range   = list(range(START_YEAR, current_year + 1))
    city_vals    = [c for _, c in _make_cumulative(city_rows, year_range)]
    other_vals   = [c for _, c in _make_cumulative(other_rows, year_range)]
    years_labels = [str(y) if y < current_year else f"{y}*" for y in year_range]

    return {'years': years_labels, 'city_values': city_vals, 'other_values': other_vals}


# ─── Slide 4: Projected Tall Building Growth ────────────────────────────────

def slide4_data(geo_id: int, geo_type: str, threshold: int):
    current_year = datetime.date.today().year
    city_filter  = _geo_filter(geo_type)

    completed_sql = f"""
        SELECT {YEAR_COL} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE {city_filter}
          AND CAST({HEIGHT_COL} AS numeric) >= %(threshold)s
          AND {STATUS_COL} IN %(comp_statuses)s
          AND {YEAR_COL} IS NOT NULL
          AND {YEAR_COL} BETWEEN %(start)s AND %(cur_yr)s
        GROUP BY yr ORDER BY yr
    """
    future_sql = f"""
        SELECT {YEAR_COL} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE {city_filter}
          AND CAST({HEIGHT_COL} AS numeric) >= %(threshold)s
          AND {STATUS_COL} NOT IN %(comp_statuses)s
          AND {YEAR_COL} IS NOT NULL
          AND {YEAR_COL} BETWEEN %(cur_yr)s AND %(future_end)s
        GROUP BY yr ORDER BY yr
    """
    params = {
        'geo_id': geo_id, 'threshold': threshold,
        'comp_statuses': COMPLETED_STATUSES,
        'start': START_YEAR, 'cur_yr': current_year,
        'future_end': current_year + 5,
    }

    with cvu_conn() as conn:
        cur = conn.cursor()
        cur.execute(completed_sql, params)
        comp_rows = dict(cur.fetchall())
        cur.execute(future_sql, params)
        future_rows = dict(cur.fetchall())

    hist_range   = list(range(START_YEAR, current_year + 1))
    future_range = list(range(current_year + 1, current_year + 6))

    hist_cum   = {y: c for y, c in _make_cumulative(comp_rows, hist_range)}
    base_total = hist_cum.get(current_year, 0)
    proj_cum   = {}
    running    = base_total
    for y in [current_year] + future_range:
        running += future_rows.get(y, 0)
        proj_cum[y] = running

    all_years  = hist_range + future_range
    hist_vals  = [hist_cum.get(y) for y in all_years]
    proj_vals  = [proj_cum.get(y) for y in all_years]

    return {
        'years':       [str(y) for y in all_years],
        'hist_values': hist_vals,
        'proj_values': proj_vals,
    }


# ─── Slide 5: Buildings + Population ────────────────────────────────────────

def slide5_data(geo_id: int, geo_type: str, threshold: int, geo_name: str):
    current_year = datetime.date.today().year
    s4 = slide4_data(geo_id, geo_type, threshold)

    # GHSL population is only available at city/agglomeration level
    pop_by_year = {}
    if geo_type in ('city', 'agglomeration'):
        with ghsl_conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    'SELECT * FROM ghsl WHERE "GC_UCN_MAI_2025" ILIKE %(name)s LIMIT 1',
                    {'name': f'%{geo_name}%'}
                )
                row = cur.fetchone()
                if row:
                    colnames = [d.name for d in cur.description]
                    for col, val in zip(colnames, row):
                        m = re.match(r'GH_POP_TOT_(\d{4})$', col)
                        if m and val is not None:
                            try:
                                pop_by_year[int(m.group(1))] = int(val)
                            except (ValueError, TypeError):
                                pass
            except Exception as e:
                log.warning("GHSL population query failed: %s", e)

    all_years    = [int(y.rstrip('*')) for y in s4['years']]
    hist_range   = [y for y in all_years if y <= current_year]
    future_range = [y for y in all_years if y > current_year]

    def _interp_pop(year_list):
        if not pop_by_year:
            return [None] * len(year_list)
        out = []
        known = sorted(pop_by_year.keys())
        for y in year_list:
            if y in pop_by_year:
                out.append(pop_by_year[y])
            else:
                before = [k for k in known if k <= y]
                after  = [k for k in known if k >= y]
                if before and after:
                    y0, y1 = before[-1], after[0]
                    if y0 == y1:
                        out.append(pop_by_year[y0])
                    else:
                        p0, p1 = pop_by_year[y0], pop_by_year[y1]
                        out.append(int(p0 + (p1 - p0) * (y - y0) / (y1 - y0)))
                elif before:
                    out.append(pop_by_year[before[-1]])
                else:
                    out.append(pop_by_year[after[0]])
        return out

    hist_pop   = _interp_pop(hist_range) + [None] * len(future_range)
    future_pop = [None] * len(hist_range) + _interp_pop(future_range)

    if hist_range and future_range:
        pivot = len(hist_range) - 1
        future_pop[pivot] = hist_pop[pivot]

    return {
        'years':       s4['years'],
        'hist_builds': s4['hist_values'],
        'proj_builds': s4['proj_values'],
        'hist_pop':    hist_pop,
        'future_pop':  future_pop,
    }


# ─── Slide 6: Building Characteristics ──────────────────────────────────────

def slide6_data(geo_id: int, geo_type: str, threshold: int):
    city_filter = _geo_filter(geo_type)
    params = {'geo_id': geo_id, 'threshold': threshold, 'statuses': COMPLETED_STATUSES}

    with cvu_conn() as conn:
        cur = conn.cursor()

        # Function: 5 fixed buckets via CASE expression
        cur.execute(f"""
            SELECT ({FUNC_EXPR}) AS cat, COUNT(*) AS cnt
            FROM ctbuh_building b
            WHERE {city_filter}
              AND CAST({HEIGHT_COL} AS numeric) >= %(threshold)s
              AND {STATUS_COL} IN %(statuses)s
              AND main_use_01 IS NOT NULL AND main_use_01::text != ''
            GROUP BY cat ORDER BY cnt DESC
        """, params)
        func_rows = cur.fetchall()

        # Material: 5 fixed buckets via CASE expression
        cur.execute(f"""
            SELECT ({MAT_EXPR}) AS cat, COUNT(*) AS cnt
            FROM ctbuh_building b
            WHERE {city_filter}
              AND CAST({HEIGHT_COL} AS numeric) >= %(threshold)s
              AND {STATUS_COL} IN %(statuses)s
              AND {MAT_COL} IS NOT NULL AND {MAT_COL}::text != ''
            GROUP BY cat ORDER BY cnt DESC
        """, params)
        mat_rows = cur.fetchall()

    def _normalise(rows):
        return [r[0] for r in rows], [int(r[1]) for r in rows]

    func_cats, func_counts = _normalise(func_rows)
    mat_cats,  mat_counts  = _normalise(mat_rows)

    return {
        'func_categories': func_cats,
        'func_values':     func_counts,
        'mat_categories':  mat_cats,
        'mat_values':      mat_counts,
    }


# ─── Geography metadata ──────────────────────────────────────────────────────

def get_city_meta(geo_id: int, geo_type: str):
    with cvu_conn() as conn:
        cur = conn.cursor()
        if geo_type == 'city':
            cur.execute("""
                SELECT c.name, co.name
                FROM v2_cities c JOIN v2_countries co ON co.id = c.country_id
                WHERE c.id = %s
            """, (geo_id,))
            row = cur.fetchone()
            if row:
                return {'city': row[0], 'country': row[1]}
        elif geo_type == 'agglomeration':
            cur.execute("SELECT name_intl FROM agglomerations WHERE id = %s", (geo_id,))
            row = cur.fetchone()
            if row:
                return {'city': row[0], 'country': ''}
        elif geo_type == 'country':
            cur.execute("SELECT name FROM v2_countries WHERE id = %s", (geo_id,))
            row = cur.fetchone()
            if row:
                return {'city': row[0], 'country': ''}
        elif geo_type == 'region':
            cur.execute("SELECT name FROM v2_regions WHERE id = %s", (geo_id,))
            row = cur.fetchone()
            if row:
                return {'city': row[0], 'country': ''}
    return {'city': 'Unknown', 'country': ''}