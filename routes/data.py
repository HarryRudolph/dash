"""
Data Feed Routes
----------------
Status and health endpoints for each data source.
Fill in query logic where marked with # TODO.

Mounted with prefix="/dashboard" in app.py.
"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from routes import templates

router = APIRouter()


@router.get("/data")
async def data_page(request: Request):
    return templates.TemplateResponse(request, "data.html")


@router.get("/data/feeds")
async def feed_status():
    """
    Returns a flat list of feeds grouped by source. Each feed has:
      - name: display name (e.g. "AIS Positions")
      - source: backend type ("elasticsearch" | "postgres" | "mongodb" | "senzing")
      - key: unique identifier (e.g. "es_ais_positions")
      - status: "up" | "down" | "unknown"
      - total_count: total records in this feed
      - last_record: ISO timestamp of most recent record
      - history: list of {timestamp, count} for volume over time (last 24h, hourly)
    """

    feeds = []

    # -- Elasticsearch indices -----------------------------------------------
    # TODO: for each index, GET /_cat/indices/<index>?format=json for doc count,
    #       GET /_cluster/health for status, query for max(@timestamp) for last_record,
    #       date_histogram agg for history

    feeds.append({
        "name": "AIS Positions",
        "source": "elasticsearch",
        "key": "es_ais_positions",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    feeds.append({
        "name": "AIS Voyages",
        "source": "elasticsearch",
        "key": "es_ais_voyages",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    feeds.append({
        "name": "Vessel Ownership",
        "source": "elasticsearch",
        "key": "es_vessel_ownership",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    # -- PostgreSQL tables ---------------------------------------------------
    # TODO: check connection, SELECT count(*) and max(updated_at) per table

    feeds.append({
        "name": "Vessel Registry",
        "source": "postgres",
        "key": "pg_vessel_registry",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    feeds.append({
        "name": "Sanctions Lists",
        "source": "postgres",
        "key": "pg_sanctions",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    # -- MongoDB collections -------------------------------------------------
    # TODO: db.collection.estimated_document_count(), check connection

    feeds.append({
        "name": "Behavioural Events",
        "source": "mongodb",
        "key": "mongo_events",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    feeds.append({
        "name": "Port Calls",
        "source": "mongodb",
        "key": "mongo_port_calls",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    # -- Senzing -------------------------------------------------------------
    # TODO: G2Engine.stats() or equivalent health check

    feeds.append({
        "name": "Entity Resolution",
        "source": "senzing",
        "key": "senzing_entities",
        "status": "unknown",
        "total_count": None,
        "last_record": None,
        "history": [],
    })

    total = sum(f["total_count"] for f in feeds if f["total_count"] is not None)

    return JSONResponse({"total_count": total, "feeds": feeds})
