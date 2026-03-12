"""
main.py — FastAPI backend for the CVU City Slides generator.

geo_type: 'city' | 'agglomeration' | 'country' | 'region'
Slide 3 (vs. other cities in country) is only available for geo_type='city'.
"""

import os
import hashlib
import logging
import subprocess
import threading
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler

import db
import queries
import pptx_gen

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PASSWORD        = os.getenv("PASSWORD")
SYNC_SCRIPT     = os.getenv("SYNC_SCRIPT", "sync_mysql_to_pg.py")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

_TOKEN     = hashlib.sha256((PASSWORD or "").encode()).hexdigest()
_sync_lock = threading.Lock()
_sync_status = {"running": False, "last_run": None, "last_status": "never"}

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="CVU Slide Generator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

_scheduler = BackgroundScheduler(timezone="UTC")


@app.on_event("startup")
def on_startup():
    db.init_pools()
    try:
        db.discover_schemas()
    except Exception as e:
        log.error("Schema discovery failed: %s", e)
    _scheduler.add_job(
        _run_sync_subprocess, trigger="cron",
        hour=3, minute=0, id="daily_sync", replace_existing=True,
    )
    _scheduler.start()
    log.info("Daily sync scheduled at 03:00 UTC")


@app.on_event("shutdown")
def on_shutdown():
    _scheduler.shutdown(wait=False)
    db.close_pools()


# ── Auth ──────────────────────────────────────────────────────────────────────

def _check_auth(authorization: str | None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header")
    if authorization.split(" ", 1)[1] != _TOKEN:
        raise HTTPException(403, "Invalid token")


# ── Sync ──────────────────────────────────────────────────────────────────────

def _run_sync_subprocess():
    with _sync_lock:
        _sync_status["running"] = True
        _sync_status["last_status"] = "running"
        try:
            result = subprocess.run(
                ["python3", SYNC_SCRIPT],
                capture_output=True, text=True, timeout=3600,
            )
            if result.returncode == 0:
                _sync_status["last_status"] = "success"
                log.info("Sync completed successfully")
            else:
                _sync_status["last_status"] = f"failed: {result.stderr[-500:]}"
                log.error("Sync failed: %s", result.stderr[-500:])
        except Exception as e:
            _sync_status["last_status"] = f"error: {e}"
            log.exception("Sync error")
        finally:
            _sync_status["running"] = False
            _sync_status["last_run"] = datetime.now(timezone.utc).isoformat()


# ── Request schemas ───────────────────────────────────────────────────────────

class AuthRequest(BaseModel):
    password: str


class GenerateRequest(BaseModel):
    geo_id:     int
    geo_type:   str              # 'city' | 'agglomeration' | 'country' | 'region'
    geo_name:   str              # display name (used for GHSL lookup on slide 5)
    country_id: Optional[int] = None   # required for slide 3 (city only)
    threshold:  int = 100
    slides:     list[int] = [2, 3, 4, 5, 6]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "time":   datetime.now(timezone.utc).isoformat(),
        "sync":   _sync_status,
    }


@app.post("/api/auth")
def authenticate(body: AuthRequest):
    if hashlib.sha256(body.password.encode()).hexdigest() != _TOKEN:
        raise HTTPException(403, "Wrong password")
    return {"token": _TOKEN}


@app.get("/api/geographies")
def geo_list(authorization: str = Header(None)):
    _check_auth(authorization)
    try:
        return queries.get_geo_list()
    except Exception as e:
        log.exception("geo_list failed")
        raise HTTPException(500, str(e))


@app.post("/api/generate")
def generate(req: GenerateRequest, authorization: str = Header(None)):
    _check_auth(authorization)

    meta     = queries.get_geo_meta(req.geo_id, req.geo_type)
    geo_name = meta["name"]
    subtitle = meta["subtitle"]   # country name for cities, empty otherwise

    # Slide 3 only available for cities
    requested_slides = [s for s in req.slides if not (s == 3 and req.geo_type != 'city')]

    slide_data = {}
    for slide_num, fn, args in [
        (2, queries.slide2_data, (req.geo_id, req.geo_type, req.threshold)),
        (3, queries.slide3_data, (req.geo_id, req.geo_type, req.country_id, req.threshold)),
        (4, queries.slide4_data, (req.geo_id, req.geo_type, req.threshold)),
        (5, queries.slide5_data, (req.geo_id, req.geo_type, req.threshold, req.geo_name)),
        (6, queries.slide6_data, (req.geo_id, req.geo_type, req.threshold)),
    ]:
        if slide_num not in requested_slides:
            continue
        if slide_num == 3 and not req.country_id:
            continue
        try:
            slide_data[f"s{slide_num}"] = fn(*args)
        except Exception as e:
            log.exception("slide%s_data failed", slide_num)
            slide_data[f"s{slide_num}"] = None

    try:
        pptx_bytes = pptx_gen.generate_pptx(
            geo_name=geo_name,
            subtitle=subtitle,
            threshold=req.threshold,
            slide_data=slide_data,
            selected_slides=requested_slides,
        )
    except Exception as e:
        log.exception("PPTX generation failed")
        raise HTTPException(500, f"PPTX generation failed: {e}")

    safe_name = geo_name.replace(" ", "_").replace("/", "-")
    filename  = f"CVU_{safe_name}_{req.threshold}m.pptx"

    return Response(
        content=pptx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/sync")
def trigger_sync(authorization: str = Header(None)):
    _check_auth(authorization)
    if _sync_status["running"]:
        return {"status": "already_running", "last_run": _sync_status["last_run"]}
    t = threading.Thread(target=_run_sync_subprocess, daemon=True)
    t.start()
    return {"status": "started"}


@app.get("/api/sync/status")
def sync_status(authorization: str = Header(None)):
    _check_auth(authorization)
    return _sync_status