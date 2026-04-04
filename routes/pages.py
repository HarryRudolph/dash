"""
Static page routes — Home, Vessels search.
Live position endpoint for the home dashboard.
"""

import hashlib
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from config import DEV_MODE, SENSOR, TILE_SERVER
from routes import templates
from services.elasticsearch import ElasticsearchError, get_latest_positions

router = APIRouter()

_MAX_MMSIS = 20
_MMSI_RE = re.compile(r"^\d{9}$")

_MOCK_NAMES = [
    "EVER GIVEN", "MAERSK TIGRIS", "PACIFIC VOYAGER", "NORDIC GRACE",
    "OCEAN TITAN", "STELLA MARIS", "CAPE ARROW", "BALTIC WIND",
    "CORAL PRINCESS", "JADE STAR", "IRON MONARCH", "SILVER BAY",
    "GOLDEN HORIZON", "EMERALD SEA", "ARCTIC SPIRIT", "BLUE MARLIN",
    "RED FALCON", "WHITE PEARL", "BLACK SWAN", "IVORY COAST",
]

_MOCK_FLAGS = [
    "PA", "LR", "MH", "SG", "HK", "MT", "BS", "GB", "NO", "GR",
    "CY", "JP", "DK", "BE", "IT", "DE", "FR", "NL", "PT", "KR",
]


def _deterministic_float(seed: str, low: float, high: float) -> float:
    """Return a stable float in [low, high) derived from a seed string."""
    h = int(hashlib.md5(seed.encode()).hexdigest()[:8], 16)
    return low + (h / 0xFFFFFFFF) * (high - low)


def _mock_positions(mmsis: list) -> list:
    """Generate moving mock vessel positions seeded by MMSI + current minute."""
    now = datetime.now(timezone.utc)
    minute_bucket = now.strftime("%Y%m%d%H%M")
    vessels = []

    for i, mmsi in enumerate(mmsis):
        base_seed = f"{mmsi}:base"
        name = _MOCK_NAMES[int(hashlib.md5(base_seed.encode()).hexdigest()[:4], 16) % len(_MOCK_NAMES)]
        flag = _MOCK_FLAGS[int(hashlib.md5(base_seed.encode()).hexdigest()[4:8], 16) % len(_MOCK_FLAGS)]

        base_lat = SENSOR.lat + _deterministic_float(f"{mmsi}:lat", -2.0, 2.0)
        base_lon = SENSOR.lon + _deterministic_float(f"{mmsi}:lon", -2.0, 2.0)

        heading = _deterministic_float(f"{mmsi}:{minute_bucket}:hdg", 0, 360)
        speed = _deterministic_float(f"{mmsi}:{minute_bucket}:spd", 1, 18)

        drift_seed = f"{mmsi}:{minute_bucket}"
        drift_lat = _deterministic_float(f"{drift_seed}:dlat", -0.005, 0.005)
        drift_lon = _deterministic_float(f"{drift_seed}:dlon", -0.005, 0.005)
        lat = base_lat + drift_lat
        lon = base_lon + drift_lon

        trail = []
        for step in range(1, 6):
            t = now - timedelta(minutes=step * 5)
            step_seed = f"{mmsi}:{t.strftime('%Y%m%d%H%M')}"
            step_dlat = _deterministic_float(f"{step_seed}:dlat", -0.005, 0.005)
            step_dlon = _deterministic_float(f"{step_seed}:dlon", -0.005, 0.005)
            trail.append({
                "lat": base_lat + step_dlat,
                "lon": base_lon + step_dlon,
                "timestamp": t.isoformat(),
            })

        vessels.append({
            "mmsi": mmsi,
            "name": name,
            "flag": flag,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "heading": round(heading, 1),
            "speed": round(speed, 1),
            "timestamp": now.isoformat(),
            "trail": [
                {"lat": round(p["lat"], 6), "lon": round(p["lon"], 6), "timestamp": p["timestamp"]}
                for p in trail
            ],
        })

    return vessels


class PositionsRequest(BaseModel):
    mmsis: list


@router.get("")
async def home(request: Request):
    return templates.TemplateResponse(
        "home.html",
        {"request": request, "tile_server": TILE_SERVER.as_dict()},
    )


@router.get("/vessels")
async def vessels(request: Request):
    return templates.TemplateResponse("vessels.html", {"request": request})


@router.post("/positions")
async def positions(request: Request, body: PositionsRequest):
    """Return latest positions for the requested MMSIs."""
    mmsis = [m for m in body.mmsis if _MMSI_RE.match(str(m))]
    mmsis = mmsis[:_MAX_MMSIS]

    if not mmsis:
        return JSONResponse({"vessels": [], "updated_at": datetime.now(timezone.utc).isoformat()})

    if DEV_MODE:
        return JSONResponse({
            "vessels": _mock_positions(mmsis),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

    # Production: query Elasticsearch via app.state.es
    try:
        vessels = await get_latest_positions(request.app.state.es, mmsis)
    except ElasticsearchError:
        vessels = []

    return JSONResponse({
        "vessels": vessels,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
