"""
Data Feed Routes
----------------
Status and health endpoints for each data source.

Mounted with prefix="/dashboard" in app.py.
"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from routes import templates
from services.elasticsearch import get_index_stats
from services.minio_client import check_health as minio_health, list_objects
from services.mongo import get_collection_stats, EVENTS_COLLECTION, PORT_CALLS_COLLECTION
from services.postgres import get_table_stats

router = APIRouter()


@router.get("/data")
async def data_page(request: Request):
    return templates.TemplateResponse("data.html", {"request": request})


@router.get("/data/feeds")
async def feed_status(request: Request):
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

    es = request.app.state.es
    db = request.app.state.db
    pg = request.app.state.pg

    feeds = []

    # -- Elasticsearch indices -----------------------------------------------
    es_ais = await get_index_stats(es, "ais_positions")
    feeds.append({
        "name": "AIS Positions",
        "source": "elasticsearch",
        "key": "es_ais_positions",
        **es_ais,
    })

    es_voyages = await get_index_stats(es, "ais_voyages")
    feeds.append({
        "name": "AIS Voyages",
        "source": "elasticsearch",
        "key": "es_ais_voyages",
        **es_voyages,
    })

    es_ownership = await get_index_stats(es, "vessel_ownership")
    feeds.append({
        "name": "Vessel Ownership",
        "source": "elasticsearch",
        "key": "es_vessel_ownership",
        **es_ownership,
    })

    # -- PostgreSQL tables ---------------------------------------------------
    pg_registry = get_table_stats(pg, "vessel_registry")
    feeds.append({
        "name": "Vessel Registry",
        "source": "postgres",
        "key": "pg_vessel_registry",
        **pg_registry,
        "history": [],
    })

    pg_sanctions = get_table_stats(pg, "sanctions")
    feeds.append({
        "name": "Sanctions Lists",
        "source": "postgres",
        "key": "pg_sanctions",
        **pg_sanctions,
        "history": [],
    })

    # -- MongoDB collections -------------------------------------------------
    mongo_events = get_collection_stats(db, EVENTS_COLLECTION)
    feeds.append({
        "name": "Behavioural Events",
        "source": "mongodb",
        "key": "mongo_events",
        **mongo_events,
        "history": [],
    })

    mongo_ports = get_collection_stats(db, PORT_CALLS_COLLECTION)
    feeds.append({
        "name": "Port Calls",
        "source": "mongodb",
        "key": "mongo_port_calls",
        **mongo_ports,
        "history": [],
    })

    # -- MinIO buckets -------------------------------------------------------
    minio = request.app.state.minio
    minio_up = minio_health(minio)
    # Example: check for data in a bucket — update bucket name to match deployment
    minio_objects = list_objects(minio, "ais-data") if minio_up else []
    feeds.append({
        "name": "AIS Blob Store",
        "source": "minio",
        "key": "minio_ais_data",
        "status": "up" if minio_up else "down",
        "total_count": len(minio_objects) if minio_up else None,
        "last_record": minio_objects[0]["last_modified"] if minio_objects else None,
        "history": [],
    })

    # -- Senzing -------------------------------------------------------------
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
