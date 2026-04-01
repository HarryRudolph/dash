"""
Vessel Dashboard Routes
-----------------------
Boilerplate route stubs. Fill in your ES/Senzing/Mongo query logic
where marked with # TODO.

Mounted with prefix="/dashboard" in app.py, so all paths below
are relative to /dashboard.
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from routes import templates
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
        request, "vessel_dashboard.html", context={"mmsi": mmsi},
    )


# ---------------------------------------------------------------------------
# Tab 1 — Overview: AIS identity + ownership summary from ES
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/overview")
async def vessel_overview(mmsi: str):
    """
    Returns JSON with two keys:
      - identity: AIS static fields
      - ownership: summary from your ownership ES index

    The frontend renders these into cards on the Overview tab.
    """
    # TODO: query your AIS index for static fields
    identity = {
        "mmsi": mmsi,
        "imo": None,
        "name": None,
        "flag": None,
        "vessel_type": None,
        "length": None,
        "beam": None,
        "draught": None,
        "destination": None,
        "last_seen": None,
    }

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
# Tab 1 — Map: last 5 days AIS track (Folium HTML)
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/map", response_class=HTMLResponse)
async def vessel_map(mmsi: str):
    """
    Returns raw HTML (Folium export) to embed in an iframe.
    """
    # TODO: query ES for last 5 days of AIS, build Folium map
    # return HTMLResponse(folium_map._repr_html_())
    return HTMLResponse("<p>Map placeholder</p>")


# ---------------------------------------------------------------------------
# Tab 2 — Network: Senzing entity graph as Cytoscape JSON
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/network")
async def vessel_network(mmsi: str):
    """
    Returns Cytoscape.js elements format:
    {
        "elements": {
            "nodes": [{"data": {"id": "n1", "label": "...", "type": "vessel|person|company"}}],
            "edges": [{"data": {"source": "n1", "target": "n2", "label": "owns"}}]
        }
    }

    Transform your Senzing response into this shape.
    The frontend renders it with Cytoscape.js.
    """
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
    """
    Expands the selected entity by fetching its related entities from Senzing.
    """
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
async def vessel_events(mmsi: str):
    """
    Returns JSON array of recent events. Frontend renders the table.
    (Returning JSON rather than pre-rendered HTML gives you sorting/filtering
    on the client side later.)

    Each event should have at minimum:
      - timestamp, event_type, summary
    Add whatever other fields are useful.
    """
    # TODO: query Mongo for last 10 events
    events = [
        # {"timestamp": "2025-03-28T14:32:00Z", "event_type": "dark_period", "summary": "AIS off for 6h", "details": {}},
    ]
    return JSONResponse(events)


# ---------------------------------------------------------------------------
# Tab 3 — Pattern of Life: 2D histogram data
# ---------------------------------------------------------------------------
@router.get("/vessel_info/{mmsi}/pattern")
async def vessel_pattern(mmsi: str):
    """
    Returns the 2D histogram as JSON for client-side rendering.

    Shape:
    {
        "x_labels": ["Mon", "Tue", ...],       # days
        "y_labels": ["00:00", "01:00", ...],    # hour bins
        "values": [[0, 3, 1, ...], ...]         # counts, row per y-bin
    }
    """
    # TODO: query ES for last year of AIS, compute histogram
    pattern = {
        "x_labels": [],
        "y_labels": [],
        "values": [],
    }
    return JSONResponse(pattern)
