"""
db.py — connection pools for both Aiven PostgreSQL databases.
"""
import os, logging
import psycopg2

log = logging.getLogger(__name__)

CVU_DSN = os.getenv(
    "CVU_DSN",
    "postgres://avnadmin:AVNS_tnbgy7eoqDmdFg-NsLA"
    "@buildingdb-buildingdb.a.aivencloud.com:13020"
    "/defaultdb?sslmode=require"
)
GHSL_DSN = os.getenv(
    "GHSL_DSN",
    "postgres://avnadmin:AVNS_6tFbHFoB3cJA1ORIbAE"
    "@vui-vui.i.aivencloud.com:15955/defaultdb?sslmode=require"
)

def init_pools():
    # Validate connections at startup; errors surface early
    for dsn, label in [(CVU_DSN, "CVU"), (GHSL_DSN, "GHSL")]:
        conn = psycopg2.connect(dsn)
        conn.close()
        log.info("%s DB reachable", label)

def close_pools():
    pass  # per-request connections close themselves

class _DirectConn:
    """Per-request connection — immune to Aiven idle-timeout after long syncs."""
    def __init__(self, dsn):
        self._dsn = dsn
        self._conn = None
    def __enter__(self):
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = True
        return self._conn
    def __exit__(self, *_):
        try:
            self._conn.close()
        except Exception:
            pass

def cvu_conn():  return _DirectConn(CVU_DSN)
def ghsl_conn(): return _DirectConn(GHSL_DSN)

# Populated at startup by discover_schemas()
BUILDING_COLS: dict = {}
GHSL_COLS:     dict = {}

def _pick(candidates, col_set):
    return next((c for c in candidates if c in col_set), None)

def discover_schemas():
    with cvu_conn() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema='public' AND table_name='ctbuh_building'""")
        b_cols = {r[0] for r in cur.fetchall()}
        log.info("ctbuh_building cols: %s", sorted(b_cols))
        BUILDING_COLS['height']   = _pick(['height_m','height','height_meters'], b_cols)
        BUILDING_COLS['year']     = _pick(['year_completion','completion_year','year_built','year'], b_cols)
        BUILDING_COLS['city_id']  = _pick(['city_id','cityid','city'], b_cols)
        BUILDING_COLS['status']   = _pick(['status','status_id','building_status'], b_cols)
        BUILDING_COLS['function'] = _pick(['primary_function','function','building_function','function_id'], b_cols)
        BUILDING_COLS['material'] = _pick(['structural_material','material','material_id'], b_cols)

        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema='public' AND table_name='v2_cities'""")
        city_cols = {r[0] for r in cur.fetchall()}
        BUILDING_COLS['city_country'] = _pick(['country_id','country'], city_cols)
        BUILDING_COLS['city_name']    = _pick(['name','city_name'], city_cols)
        log.info("Mapped building cols: %s", BUILDING_COLS)

    with ghsl_conn() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema='public' AND table_name='ghsl'""")
        g_cols = {r[0] for r in cur.fetchall()}
        log.info("ghsl cols: %s", sorted(g_cols))
        GHSL_COLS['city_id']    = _pick(['city_id','id_city','fid','id'], g_cols)
        GHSL_COLS['city_name']  = _pick(['city_name','name','uc_nm_lst','nm_main'], g_cols)
        GHSL_COLS['year']       = _pick(['year','yr'], g_cols)
        GHSL_COLS['population'] = _pick(['population','pop','p_2015','pop_2015'], g_cols)
        log.info("Mapped GHSL cols: %s", GHSL_COLS)