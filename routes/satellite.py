"""
Satellite Pass Routes
---------------------
SAR satellite overhead detection and pass history.
Fill in query logic where marked with # TODO.

Data model (from Elasticsearch):
  - norad_id: NORAD catalog number
  - satellite: display name
  - detection_time: ISO timestamp
  - tle_line1 / tle_line2: TLE at time of detection
  - max_elevation, imaging window, etc.

Mounted with prefix="/dashboard" in app.py.
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from config import DEV_MODE, SATVIS, SENSOR, TILE_SERVER
from routes import templates

router = APIRouter()
satvis_router = APIRouter()


def _mock_passes():
    """Generate realistic mock passes for local development."""
    now = datetime.now(timezone.utc)
    return [
        {
            "satellite": "SENTINEL-1A",
            "norad_id": 39634,
            "pass_start": (now - timedelta(hours=1, minutes=22)).isoformat(),
            "pass_end": (now - timedelta(hours=1, minutes=14)).isoformat(),
            "max_elevation": 72.3,
            "imaging": True,
            "imaging_start": (now - timedelta(hours=1, minutes=20)).isoformat(),
            "imaging_end": (now - timedelta(hours=1, minutes=16)).isoformat(),
            "tle_line1": "1 39634U 14016A   26091.50000000  .00000043  00000-0  36789-4 0  9991",
            "tle_line2": "2 39634  98.1817 126.5243 0001312  91.2378 268.8946 14.59198515625012",
        },
        {
            "satellite": "SENTINEL-1B",
            "norad_id": 41456,
            "pass_start": (now - timedelta(hours=3, minutes=47)).isoformat(),
            "pass_end": (now - timedelta(hours=3, minutes=40)).isoformat(),
            "max_elevation": 45.1,
            "imaging": False,
            "imaging_start": None,
            "imaging_end": None,
            "tle_line1": "1 41456U 16025A   26091.50000000  .00000038  00000-0  32145-4 0  9993",
            "tle_line2": "2 41456  98.1812 126.4987 0001205  89.5432 270.5892 14.59197823487651",
        },
        {
            "satellite": "COSMO-SKYMED 1",
            "norad_id": 31598,
            "pass_start": (now - timedelta(hours=5, minutes=10)).isoformat(),
            "pass_end": (now - timedelta(hours=5, minutes=3)).isoformat(),
            "max_elevation": 61.8,
            "imaging": True,
            "imaging_start": (now - timedelta(hours=5, minutes=8)).isoformat(),
            "imaging_end": (now - timedelta(hours=5, minutes=5)).isoformat(),
            "tle_line1": "1 31598U 07023A   26091.50000000  .00000112  00000-0  54321-4 0  9997",
            "tle_line2": "2 31598  97.8867 164.1234 0001567  73.2145 286.9123 14.82134567891234",
        },
        {
            "satellite": "TERRASAR-X",
            "norad_id": 31698,
            "pass_start": (now - timedelta(hours=8, minutes=33)).isoformat(),
            "pass_end": (now - timedelta(hours=8, minutes=27)).isoformat(),
            "max_elevation": 34.6,
            "imaging": True,
            "imaging_start": (now - timedelta(hours=8, minutes=31)).isoformat(),
            "imaging_end": (now - timedelta(hours=8, minutes=29)).isoformat(),
            "tle_line1": "1 31698U 07026A   26091.50000000  .00000089  00000-0  47891-4 0  9994",
            "tle_line2": "2 31698  97.4512 178.9876 0001834  82.4567 277.6789 15.19148723456789",
        },
        {
            "satellite": "ICEYE-X2",
            "norad_id": 43800,
            "pass_start": (now - timedelta(hours=11, minutes=5)).isoformat(),
            "pass_end": (now - timedelta(hours=10, minutes=58)).isoformat(),
            "max_elevation": 52.9,
            "imaging": False,
            "imaging_start": None,
            "imaging_end": None,
            "tle_line1": "1 43800U 18099A   26091.50000000  .00000076  00000-0  41234-4 0  9992",
            "tle_line2": "2 43800  97.5234 145.6789 0001456  67.8912 292.2345 15.07234567123456",
        },
        {
            "satellite": "SENTINEL-1A",
            "norad_id": 39634,
            "pass_start": (now - timedelta(hours=13, minutes=45)).isoformat(),
            "pass_end": (now - timedelta(hours=13, minutes=37)).isoformat(),
            "max_elevation": 58.4,
            "imaging": True,
            "imaging_start": (now - timedelta(hours=13, minutes=43)).isoformat(),
            "imaging_end": (now - timedelta(hours=13, minutes=39)).isoformat(),
            "tle_line1": "1 39634U 14016A   26091.50000000  .00000043  00000-0  36789-4 0  9991",
            "tle_line2": "2 39634  98.1817 126.5243 0001312  91.2378 268.8946 14.59198515625012",
        },
        {
            "satellite": "COSMO-SKYMED 2",
            "norad_id": 33412,
            "pass_start": (now - timedelta(hours=16, minutes=12)).isoformat(),
            "pass_end": (now - timedelta(hours=16, minutes=5)).isoformat(),
            "max_elevation": 41.2,
            "imaging": True,
            "imaging_start": (now - timedelta(hours=16, minutes=10)).isoformat(),
            "imaging_end": (now - timedelta(hours=16, minutes=7)).isoformat(),
            "tle_line1": "1 33412U 08052A   26091.50000000  .00000098  00000-0  51234-4 0  9996",
            "tle_line2": "2 33412  97.8901 162.3456 0001678  71.4567 288.5678 14.82567891234567",
        },
    ]


def _mock_next_pass():
    now = datetime.now(timezone.utc)
    return {
        "satellite": "SENTINEL-1B",
        "norad_id": 41456,
        "predicted_start": (now + timedelta(hours=2, minutes=18)).isoformat(),
    }


@router.get("/satellite")
async def satellite_page(request: Request):
    return templates.TemplateResponse(
        request, "satellite.html",
        context={
            "sensor": SENSOR.as_dict(),
            "satvis_layer": TILE_SERVER.satvis_layer,
            "satvis_config": SATVIS.as_dict(),
        },
    )


@router.get("/satellite/passes")
async def satellite_passes(
    hours: int = Query(24, ge=1, le=168),
):
    """
    Returns SAR satellite passes detected at the sensor within the
    given time window (default last 24h).

    Each pass has:
      - satellite: display name
      - norad_id: NORAD catalog number
      - pass_start / pass_end: ISO timestamps of the overhead window
      - max_elevation: peak elevation angle in degrees
      - imaging: whether imaging was detected
      - imaging_start / imaging_end: imaging window timestamps (null if no imaging)
      - tle_line1 / tle_line2: TLE at time of detection
    """

    if DEV_MODE:
        return JSONResponse({
            "sensor": SENSOR.as_dict(),
            "hours": hours,
            "next_pass": _mock_next_pass(),
            "passes": _mock_passes(),
        })

    # TODO: query Elasticsearch for detections within the last `hours` hours
    # Example query shape:
    #   {
    #     "query": { "range": { "detection_time": { "gte": f"now-{hours}h" } } },
    #     "sort": [{ "detection_time": "desc" }],
    #     "_source": ["norad_id", "satellite", "detection_time", "max_elevation",
    #                  "imaging", "imaging_start", "imaging_end",
    #                  "tle_line1", "tle_line2"]
    #   }
    passes = []

    # TODO: predict next pass from TLE propagation or lookup
    next_pass = None

    return JSONResponse({
        "sensor": SENSOR.as_dict(),
        "hours": hours,
        "next_pass": next_pass,
        "passes": passes,
    })


@satvis_router.get("/satvis/tileconfig.json")
async def satvis_tile_config():
    return JSONResponse(TILE_SERVER.as_dict())
