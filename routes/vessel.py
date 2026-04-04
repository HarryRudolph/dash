"""
Vessel Dashboard Routes
-----------------------
Mounted with prefix="/dashboard" in app.py, so all paths below
are relative to /dashboard.
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from routes import templates
from services.elasticsearch import get_vessel_identity, get_vessel_track
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
        {"request": request, "mmsi": mmsi},
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

    # Build a simple Leaflet map inline
    lats = [p["lat"] for p in points]
    lons = [p["lon"] for p in points]
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)
    coords_js = ",".join(f"[{p['lat']},{p['lon']}]" for p in points)
    last = points[-1]

    html = f"""<!DOCTYPE html>
<html><head>
<link rel="stylesheet" href="/dashboard/static/lib/leaflet/leaflet.css">
<script src="/dashboard/static/lib/leaflet/leaflet.js"></script>
<style>body{{margin:0}}#m{{width:100%;height:100vh}}</style>
</head><body>
<div id="m"></div>
<script>
var m=L.map('m').setView([{center_lat},{center_lon}],8);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{maxZoom:18}}).addTo(m);
L.polyline([{coords_js}],{{color:'#4c7fd1',weight:2}}).addTo(m);
L.circleMarker([{last['lat']},{last['lon']}],{{radius:5,color:'#69a7ff',fillOpacity:1}}).addTo(m);
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
