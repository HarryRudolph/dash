"""
Vessel Dashboard Routes
-----------------------
Mounted with prefix="/dashboard" in app.py, so all paths below
are relative to /dashboard.
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

import h3

from config import TILE_SERVER
from routes import templates
from services.elasticsearch import get_vessel_identity, get_vessel_track
from services.minio_client import get_json as minio_get_json
from services.mongo import get_vessel_events
from services.senzing import (
    SenzingClient,
    SenzingError,
    build_cytoscape_graph,
)

router = APIRouter()
senzing_client = SenzingClient()


def _stub_network(mmsi: str) -> dict:
    return {
        "elements": {
            "nodes": [
                {
                    "data": {
                        "id": f"entity:{mmsi}",
                        "entity_id": mmsi,
                        "label": f"MMSI {mmsi}",
                        "type": "vessel",
                        "record_count": 1,
                        "data_sources": [],
                        "highlighted": True,
                    }
                },
            ],
            "edges": [],
        },
        "meta": {
            "source": "stub",
            "seed_entity_id": None,
            "warning": "Set SZ_API_URL and SZ_DATA_SOURCE to enable Senzing graph exploration.",
            "expandable": False,
        },
    }


# ---------------------------------------------------------------------------
# Landing page — serves the tabbed template
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}", response_class=HTMLResponse)
async def vessel_dashboard(request: Request, mmsi: str):
    return templates.TemplateResponse(
        "vessel_dashboard.html",
        {"request": request, "mmsi": mmsi, "tile_server": TILE_SERVER.as_dict()},
    )


# ---------------------------------------------------------------------------
# Tab 1 — Overview: AIS identity + ownership summary from ES
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/overview")
async def vessel_overview(request: Request, mmsi: str):
    es = request.app.state.es

    ais = await get_vessel_identity(es, mmsi)
    identity = {
        "mmsi": mmsi,
        "imo": None,
        "name": None,
        "flag": None,
        "vessel_type": None,
        "length": None,
        "destination": None,
        "last_seen": None,
    }
    if ais:
        identity.update({
            "imo": ais.get("imo"),
            "name": ais.get("name"),
            "flag": ais.get("flag"),
            "vessel_type": ais.get("vessel_type"),
            "length": ais.get("length"),
            "destination": ais.get("destination"),
            "last_seen": ais.get("last_seen"),
        })

    # TODO: query your ownership ES index
    ownership = {
        "registered_owner": None,
        "operator": None,
        "group_owner": None,
        "flag_state": None,
        "classification_society": None,
    }

    return JSONResponse({"identity": identity, "ownership": ownership})


# ---------------------------------------------------------------------------
# Tab 1 — Map: last 5 days AIS track
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/map", response_class=HTMLResponse)
async def vessel_map(request: Request, mmsi: str):
    es = request.app.state.es
    points = await get_vessel_track(es, mmsi, hours=120)

    if not points:
        return HTMLResponse("<p style='color:#8d99ab;padding:2rem;'>No track data available</p>")

    # Build a simple Leaflet map inline — ensure coords are floats
    tile_url = TILE_SERVER.url
    tile_max = TILE_SERVER.maximum_level

    def _f(v):
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    valid = [p for p in points if _f(p["lat"]) is not None and _f(p["lon"]) is not None]
    if not valid:
        return HTMLResponse("<p style='color:#8d99ab;padding:2rem;'>No valid coordinates in track</p>")

    lats = [_f(p["lat"]) for p in valid]
    lons = [_f(p["lon"]) for p in valid]
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)
    coords_js = ",".join(f"[{_f(p['lat'])},{_f(p['lon'])}]" for p in valid)
    last = valid[-1]

    tile_layer = f"L.tileLayer('{tile_url}',{{maxZoom:{tile_max}}})" if tile_url else "L.tileLayer('')"

    html = f"""<!DOCTYPE html>
<html><head>
<link rel="stylesheet" href="/dashboard/static/lib/leaflet/leaflet.css">
<script src="/dashboard/static/lib/leaflet/leaflet.js"></script>
<style>body{{margin:0}}#m{{width:100%;height:100vh}}</style>
</head><body>
<div id="m"></div>
<script>
var m=L.map('m').setView([{center_lat},{center_lon}],8);
{tile_layer}.addTo(m);
L.polyline([{coords_js}],{{color:'#4c7fd1',weight:2}}).addTo(m);
L.circleMarker([{_f(last['lat'])},{_f(last['lon'])}],{{radius:5,color:'#69a7ff',fillOpacity:1}}).addTo(m);
</script></body></html>"""
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# Tab 2 — Network: Senzing entity graph as Cytoscape JSON
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/network")
async def vessel_network(mmsi: str):
    if not senzing_client.enabled:
        return JSONResponse(_stub_network(mmsi))

    try:
        payload = senzing_client.get_entity_by_record_id(mmsi)
        graph = build_cytoscape_graph(
            payload,
            focus_entity_id=None,
            focus_label=f"MMSI {mmsi}",
            focus_type="vessel",
        )
    except SenzingError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return JSONResponse({
        "elements": graph.elements,
        "meta": {
            "source": "senzing",
            "seed_entity_id": graph.meta.get("focus_entity_id"),
            "warning": None,
            "expandable": True,
            **graph.meta,
        },
    })


@router.get("/vessel_info/{mmsi}/network/expand/{entity_id}")
async def vessel_network_expand(mmsi: str, entity_id: int):
    if not senzing_client.enabled:
        return JSONResponse({
            "elements": {"nodes": [], "edges": []},
            "meta": {
                "source": "stub",
                "expanded_entity_id": entity_id,
                "warning": "Senzing graph expansion is not configured.",
                "expandable": False,
            },
        })

    try:
        payload = senzing_client.get_entity_by_entity_id(entity_id)
        graph = build_cytoscape_graph(
            payload,
            focus_entity_id=entity_id,
        )
    except SenzingError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return JSONResponse({
        "elements": graph.elements,
        "meta": {
            "source": "senzing",
            "expanded_entity_id": entity_id,
            "warning": None,
            "expandable": True,
            **graph.meta,
        },
    })


# ---------------------------------------------------------------------------
# Tab 3 — Events: recent behavioural events from Mongo
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/events")
async def vessel_events(request: Request, mmsi: str):
    db = request.app.state.db
    events = get_vessel_events(db, mmsi, limit=10)
    return JSONResponse(events)


# ---------------------------------------------------------------------------
# Tab 3 — Events heatmap: H3 choropleth GeoJSON
# ---------------------------------------------------------------------------
# MinIO layout:  <bucket>/h3/<mmsi>/events.json
# File format:   {"resolution": 8, "cells": {"h3_index": count, ...}}
#
# Data is pre-computed at a high resolution (e.g. 8).  The endpoint
# downsamples to whatever the client requests via ?resolution=.

H3_BUCKET = "analytics"
H3_STORED_RESOLUTION = 8


def _downsample_cells(
    cells: dict[str, int], stored_res: int, target_res: int,
) -> dict[str, int]:
    """Roll up high-res H3 cells to a coarser resolution."""
    if target_res >= stored_res:
        return cells
    out: dict[str, int] = {}
    for cell, count in cells.items():
        parent = h3.cell_to_parent(cell, target_res)
        out[parent] = out.get(parent, 0) + count
    return out


def _cells_to_geojson(cells: dict[str, int]) -> dict:
    """Convert {h3_index: count} to a GeoJSON FeatureCollection."""
    features = []
    for cell, count in cells.items():
        boundary = h3.cell_to_boundary(cell)
        # h3 returns [(lat, lng), ...], GeoJSON needs [[lng, lat], ...]
        coords = [[lng, lat] for lat, lng in boundary]
        coords.append(coords[0])  # close ring
        features.append({
            "type": "Feature",
            "properties": {"h3_index": cell, "count": count},
            "geometry": {"type": "Polygon", "coordinates": [coords]},
        })
    return {"type": "FeatureCollection", "features": features}


@router.get("/vessel_info/{mmsi}/events/heatmap")
async def vessel_events_heatmap(
    request: Request, mmsi: str, resolution: int = 5,
):
    """Read pre-computed H3 cells from MinIO, downsample to the
    requested resolution, and return GeoJSON + counts."""
    minio = request.app.state.minio
    resolution = max(1, min(resolution, H3_STORED_RESOLUTION))

    payload = minio_get_json(minio, H3_BUCKET, f"h3/{mmsi}/events.json")

    if not payload or not payload.get("cells"):
        return JSONResponse({"type": "FeatureCollection", "features": []})

    stored_res = payload.get("resolution", H3_STORED_RESOLUTION)
    cells = payload["cells"]

    if resolution < stored_res:
        cells = _downsample_cells(cells, stored_res, resolution)

    return JSONResponse(_cells_to_geojson(cells))


# ---------------------------------------------------------------------------
# Tab 3 — Pattern of Life: 2D histogram data
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/pattern")
async def vessel_pattern(mmsi: str):
    # TODO: query ES for last year of AIS, compute histogram
    pattern = {
        "x_labels": [],
        "y_labels": [],
        "values": [],
    }
    return JSONResponse(pattern)
