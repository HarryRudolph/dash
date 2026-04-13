"""MongoDB service helpers.

The MongoClient is stored on app.state.mongo / app.state.db at startup.
Pass app.state.db to these functions.

Update collection names below to match your deployment.
"""

from __future__ import annotations

from typing import Any


# -- Collection names (update these) -----------------------------------------
EVENTS_COLLECTION = "events"          # behavioural events
PORT_CALLS_COLLECTION = "port_calls"  # port call records


def get_vessel_events(db, mmsi: str, limit: int = 10) -> list[dict[str, Any]]:
    """Return the most recent behavioural events for a vessel."""
    if db is None:
        return []
    try:
        cursor = (
            db[EVENTS_COLLECTION]
            .find({"mmsi": mmsi}, {"_id": 0})
            .sort("timestamp", -1)
            .limit(limit)
        )
        return list(cursor)
    except Exception:
        return []


def get_vessel_events_with_location(
    db, mmsi: str, limit: int = 500
) -> list[dict[str, Any]]:
    """Return recent events that have lat/lon coordinates."""
    if db is None:
        return []
    try:
        cursor = (
            db[EVENTS_COLLECTION]
            .find(
                {"mmsi": mmsi, "lat": {"$exists": True}, "lon": {"$exists": True}},
                {"_id": 0},
            )
            .sort("timestamp", -1)
            .limit(limit)
        )
        return list(cursor)
    except Exception:
        return []


def get_collection_stats(db, collection: str) -> dict[str, Any]:
    """Return estimated doc count and status for a MongoDB collection."""
    if db is None:
        return {"status": "down", "total_count": None, "last_record": None}
    try:
        count = db[collection].estimated_document_count()
        return {"status": "up", "total_count": count, "last_record": None}
    except Exception:
        return {"status": "down", "total_count": None, "last_record": None}
