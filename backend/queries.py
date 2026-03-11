"""
queries.py — all SQL data retrieval for each slide.

Each function returns a dict ready for pptx_gen.py.
"""
import logging
from db import cvu_conn, ghsl_conn

log = logging.getLogger(__name__)

START_YEAR = 1975
COMPLETED_STATUSES = ('COM',)


# ─── City / agglomeration lists ─────────────────────────────────────────────

def get_city_list():
    """Return merged list of cities and agglomerations for the picker."""
    with cvu_conn() as conn:
        cur = conn.cursor()
        # Cities
        cur.execute("""
            SELECT c.id, c.name, co.name as country_name, co.id as country_id
            FROM v2_cities c
            JOIN v2_countries co ON co.id = c.country_id
            ORDER BY c.name
        """)
        cities = [
            {"type": "city", "id": r[0], "name": r[1],
             "country": r[2], "country_id": r[3]}
            for r in cur.fetchall()
        ]

        # Agglomerations
        cur.execute("""
            SELECT a.id, a.name_intl as name
            FROM agglomerations a
            ORDER BY a.name_intl
        """)
        agglos = [
            {"type": "agglomeration", "id": r[0], "name": r[1],
             "country": None, "country_id": None}
            for r in cur.fetchall()
        ]
    return cities + agglos


# ─── Column constants ────────────────────────────────────────────────────────
# Hardcoded to match actual ctbuh_building schema
HEIGHT_COL = 'height_architecture'
YEAR_COL   = 'completed'          # integer column
CITY_COL   = 'city_id'
STATUS_COL = 'status'
MAT_COL    = 'structural_material'

# Function: cast both enum columns to text so 'Mixed-use' string is valid
FUNC_EXPR  = "CASE WHEN main_use_02::text IS NULL OR main_use_02::text = '' THEN main_use_01::text ELSE 'Mixed-use' END"


def _make_cumulative(yearly_dict, year_range):
    """
    Given {year: count}, return list of (year, cumulative) over year_range.
    Missing years keep the previous cumulative value.
    """
    result = []
    running = 0
    for y in year_range:
        running += yearly_dict.get(y, 0)
        result.append((y, running))
    return result


# ─── Slide 2: 50 Years of Cumulative Growth ─────────────────────────────────

def slide2_data(city_id: int, city_type: str, threshold: int):
    h = HEIGHT_COL; yr = YEAR_COL; cid = CITY_COL; st = STATUS_COL

    if city_type == 'city':
        city_filter = f"b.{cid} = %(city_id)s"
    else:
        # Agglomeration: join through agglomeration_countries → v2_cities
        city_filter = f"""
            b.{cid} IN (
                SELECT c.id FROM v2_cities c
                JOIN agglomeration_countries ac ON ac.country_id = c.country_id
                WHERE ac.agglomeration_id = %(city_id)s
            )
        """

    sql = f"""
        SELECT {yr} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE {city_filter}
          AND CAST({h} AS numeric) >= %(threshold)s
          AND {st} IN %(statuses)s
          AND {yr} IS NOT NULL
          AND {yr} BETWEEN %(start)s AND %(end_yr)s
        GROUP BY yr
        ORDER BY yr
    """
    import datetime; current_year = datetime.date.today().year

    with cvu_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, {
            'city_id': city_id, 'threshold': threshold,
            'statuses': COMPLETED_STATUSES,
            'start': START_YEAR, 'end_yr': current_year
        })
        rows = dict(cur.fetchall())

    year_range = list(range(START_YEAR, current_year + 1))
    cumulative = _make_cumulative(rows, year_range)

    # Mark current/previous year as provisional
    years_labels = [str(y) if y < current_year else f"{y}*" for y in year_range]
    values       = [c for _, c in cumulative]

    val_2000 = next((c for y, c in cumulative if y == 2000), 0)
    val_now  = values[-1] if values else 0
    growth_pct = round((val_now - val_2000) / val_2000 * 100) if val_2000 else 0

    return {
        'years': years_labels,
        'values': values,
        'growth_pct_2000': growth_pct,
    }


# ─── Slide 3: City vs Other Cities in Country ───────────────────────────────

def slide3_data(city_id: int, city_type: str, country_id: int, threshold: int):
    import datetime; current_year = datetime.date.today().year
    h = HEIGHT_COL; yr = YEAR_COL; cid = CITY_COL; st = STATUS_COL

    if city_type != 'city':
        # For agglomerations, skip this slide (or treat agglom as city)
        return None

    base = f"""
        SELECT {yr} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE CAST({h} AS numeric) >= %(threshold)s
          AND {st} IN %(statuses)s
          AND {yr} IS NOT NULL
          AND {yr} BETWEEN %(start)s AND %(end_yr)s
    """
    params = {
        'city_id': city_id, 'country_id': country_id,
        'threshold': threshold, 'statuses': COMPLETED_STATUSES,
        'start': START_YEAR, 'end_yr': current_year
    }

    with cvu_conn() as conn:
        cur = conn.cursor()

        cur.execute(base + f" AND b.{cid} = %(city_id)s GROUP BY yr ORDER BY yr", params)
        city_rows = dict(cur.fetchall())

        cur.execute(base + f"""
            AND b.{cid} IN (
                SELECT id FROM v2_cities WHERE country_id = %(country_id)s
                AND id != %(city_id)s
            )
            GROUP BY yr ORDER BY yr
        """, params)
        other_rows = dict(cur.fetchall())

    year_range  = list(range(START_YEAR, current_year + 1))
    city_vals   = [c for _, c in _make_cumulative(city_rows, year_range)]
    other_vals  = [c for _, c in _make_cumulative(other_rows, year_range)]
    years_labels = [str(y) if y < current_year else f"{y}*" for y in year_range]

    return {
        'years':       years_labels,
        'city_values': city_vals,
        'other_values': other_vals,
    }


# ─── Slide 4: Projected Tall Building Growth ────────────────────────────────

def slide4_data(city_id: int, city_type: str, threshold: int):
    import datetime; current_year = datetime.date.today().year
    h = HEIGHT_COL; yr = YEAR_COL; cid = CITY_COL; st = STATUS_COL

    if city_type == 'city':
        city_filter = f"b.{cid} = %(city_id)s"
    else:
        city_filter = f"""b.{cid} IN (
            SELECT c.id FROM v2_cities c
            JOIN agglomeration_countries ac ON ac.country_id = c.country_id
            WHERE ac.agglomeration_id = %(city_id)s)"""

    completed_sql = f"""
        SELECT {yr} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE {city_filter}
          AND CAST({h} AS numeric) >= %(threshold)s
          AND {st} IN %(comp_statuses)s
          AND {yr} IS NOT NULL
          AND {yr} BETWEEN %(start)s AND %(cur_yr)s
        GROUP BY yr ORDER BY yr
    """
    future_sql = f"""
        SELECT {yr} AS yr, COUNT(*) AS cnt
        FROM ctbuh_building b
        WHERE {city_filter}
          AND CAST({h} AS numeric) >= %(threshold)s
          AND {st} NOT IN %(comp_statuses)s
          AND {yr} IS NOT NULL
          AND {yr} BETWEEN %(cur_yr)s AND %(future_end)s
        GROUP BY yr ORDER BY yr
    """

    params = {
        'city_id': city_id, 'threshold': threshold,
        'comp_statuses': COMPLETED_STATUSES,
        'start': START_YEAR, 'cur_yr': current_year,
        'future_end': current_year + 5
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
    # Projected: add future completions on top of current total
    base_total = hist_cum.get(current_year, 0)
    proj_cum   = {}
    running    = base_total
    for y in [current_year] + future_range:
        running += future_rows.get(y, 0)
        proj_cum[y] = running

    # Build parallel year axis: historical | overlap at current_year | future
    all_years = hist_range + future_range
    hist_vals  = [hist_cum.get(y) for y in all_years]
    proj_vals  = [proj_cum.get(y) for y in all_years]

    return {
        'years':      [str(y) for y in all_years],
        'hist_values': hist_vals,
        'proj_values': proj_vals,
    }


# ─── Slide 5: Buildings + Population ────────────────────────────────────────

def slide5_data(city_id: int, city_type: str, threshold: int, city_name: str):
    import datetime; current_year = datetime.date.today().year
    # Reuse slide4 data for building series
    s4 = slide4_data(city_id, city_type, threshold)

    # Get population from GHSL (wide format: GH_POP_TOT_1975 … GH_POP_TOT_2030)
    # City name column is GC_UCN_MAI_2025
    pop_by_year = {}
    with ghsl_conn() as conn:
        cur = conn.cursor()
        try:
            import re
            cur.execute(
                'SELECT * FROM ghsl WHERE "GC_UCN_MAI_2025" ILIKE %(name)s LIMIT 1',
                {'name': f'%{city_name}%'}
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

    all_years = [int(y.rstrip('*')) for y in s4['years']]
    hist_range   = [y for y in all_years if y <= current_year]
    future_range = [y for y in all_years if y > current_year]

    # Interpolate/extrapolate population
    def _interp_pop(year_list):
        out = []
        known_years = sorted(pop_by_year.keys())
        for y in year_list:
            if y in pop_by_year:
                out.append(pop_by_year[y])
            elif known_years:
                # Linear interpolation between nearest known
                before = [k for k in known_years if k <= y]
                after  = [k for k in known_years if k >= y]
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
            else:
                out.append(None)
        return out

    hist_pop   = _interp_pop(hist_range) + [None] * len(future_range)
    future_pop = [None] * len(hist_range) + _interp_pop(future_range)

    # Overlap at current_year
    if hist_range and future_range:
        pivot = len(hist_range) - 1
        future_pop[pivot] = hist_pop[pivot]

    return {
        'years':        s4['years'],
        'hist_builds':  s4['hist_values'],
        'proj_builds':  s4['proj_values'],
        'hist_pop':     hist_pop,
        'future_pop':   future_pop,
    }


# ─── Slide 6: Building Characteristics ──────────────────────────────────────

def slide6_data(city_id: int, city_type: str, threshold: int):
    h = HEIGHT_COL; cid = CITY_COL; st = STATUS_COL

    if city_type == 'city':
        city_filter = f"b.{cid} = %(city_id)s"
    else:
        city_filter = f"""b.{cid} IN (
            SELECT c.id FROM v2_cities c
            JOIN agglomeration_countries ac ON ac.country_id = c.country_id
            WHERE ac.agglomeration_id = %(city_id)s)"""

    params = {'city_id': city_id, 'threshold': threshold, 'statuses': COMPLETED_STATUSES}

    with cvu_conn() as conn:
        cur = conn.cursor()

        # Fix 5: derive function from main_use_01 / main_use_02
        cur.execute(f"""
            SELECT ({FUNC_EXPR}) AS cat, COUNT(*) AS cnt
            FROM ctbuh_building b
            WHERE {city_filter}
              AND CAST({h} AS numeric) >= %(threshold)s
              AND {st} IN %(statuses)s
              AND main_use_01 IS NOT NULL AND main_use_01::text != ''
            GROUP BY cat ORDER BY cnt DESC LIMIT 8
        """, params)
        func_rows = cur.fetchall()

        # Fix 6: structural_material column
        cur.execute(f"""
            SELECT COALESCE({MAT_COL}::text, 'Other') AS cat, COUNT(*) AS cnt
            FROM ctbuh_building b
            WHERE {city_filter}
              AND CAST({h} AS numeric) >= %(threshold)s
              AND {st} IN %(statuses)s
              AND {MAT_COL} IS NOT NULL AND {MAT_COL} != ''
            GROUP BY cat ORDER BY cnt DESC LIMIT 8
        """, params)
        mat_rows = cur.fetchall()

    def _normalise(rows):
        cats   = [r[0] for r in rows]
        counts = [int(r[1]) for r in rows]
        return cats, counts

    func_cats, func_counts = _normalise(func_rows)
    mat_cats,  mat_counts  = _normalise(mat_rows)

    return {
        'func_categories': func_cats,
        'func_values':     func_counts,
        'mat_categories':  mat_cats,
        'mat_values':      mat_counts,
    }


# ─── City metadata ───────────────────────────────────────────────────────────

def get_city_meta(city_id: int, city_type: str):
    with cvu_conn() as conn:
        cur = conn.cursor()
        if city_type == 'city':
            cur.execute("""
                SELECT c.name, co.name as country
                FROM v2_cities c JOIN v2_countries co ON co.id = c.country_id
                WHERE c.id = %s
            """, (city_id,))
            row = cur.fetchone()
            if row:
                return {'city': row[0], 'country': row[1]}
        else:
            cur.execute("""
                SELECT name_intl as name
                FROM agglomerations WHERE id = %s
            """, (city_id,))
            row = cur.fetchone()
            if row:
                return {'city': row[0], 'country': ''}
    return {'city': 'Unknown', 'country': ''}
