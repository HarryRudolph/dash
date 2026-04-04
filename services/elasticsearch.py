"""Elasticsearch service for AIS position queries.

Uses the async elasticsearch-py client stored on app.state.es.
Auth (basic_auth), verify_certs=False, and ssl_show_warn=False are
configured in app.py startup.

AIS index schema (fields are mostly text/strings):
    @timestamp, Course, Destination, DTG, ETA, Flag, IMO, Length,
    location (geo_point: {"coordinates": [lon, lat], "type": "Point"}),
    MMSI, Name, Speed, Status, Type

MMSI is a text field — collapse is not supported, so we use a terms
aggregation with top_hits instead.
"""

from __future__ import annotations

from typing import Any

from config import ELASTICSEARCH


class ElasticsearchError(RuntimeError):
    """Raised when an Elasticsearch request or response is invalid."""


async def get_latest_positions(es, mmsis: list[str]) -> list[dict[str, Any]]:
    """Return the latest position and a 5-point trail for each MMSI.

    Args:
        es: AsyncElasticsearch instance from app.state.es
        mmsis: list of 9-digit MMSI strings
    """
    if es is None:
        raise ElasticsearchError("Elasticsearch is not configured.")

    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"MMSI.keyword": mmsis}},
                ]
            }
        },
        "aggs": {
            "by_mmsi": {
                "terms": {
                    "field": "MMSI.keyword",
                    "size": len(mmsis),
                },
                "aggs": {
                    "latest": {
                        "top_hits": {
                            "size": 1,
                            "sort": [{"@timestamp": "desc"}],
                            "_source": [
                                "MMSI", "Name", "Flag", "Course", "Speed",
                                "location", "@timestamp", "DTG",
                                "Destination", "IMO", "Status", "Type",
                                "Length", "ETA",
                            ],
                        }
                    },
                    "trail": {
                        "top_hits": {
                            "size": 5,
                            "sort": [{"@timestamp": "desc"}],
                            "_source": ["location", "@timestamp"],
                        }
                    },
                },
            }
        },
    }

    try:
        resp = await es.search(index=ELASTICSEARCH.ais_index, body=body)
    except Exception as exc:
        raise ElasticsearchError(f"Elasticsearch query failed: {exc}") from exc

    buckets = (
        resp.get("aggregations", {})
        .get("by_mmsi", {})
        .get("buckets", [])
    )

    vessels: list[dict[str, Any]] = []
    for bucket in buckets:
        latest_hits = bucket.get("latest", {}).get("hits", {}).get("hits", [])
        if not latest_hits:
            continue
        src = latest_hits[0].get("_source", {})

        # location is geo_point: {"coordinates": [lon, lat], "type": "Point"}
        location = src.get("location", {})
        coords = location.get("coordinates", [None, None]) if isinstance(location, dict) else [None, None]
        lon = coords[0] if len(coords) > 0 else None
        lat = coords[1] if len(coords) > 1 else None

        # Build trail from trail agg
        trail_hits = bucket.get("trail", {}).get("hits", {}).get("hits", [])
        trail = []
        for th in trail_hits:
            ts = th.get("_source", {})
            t_loc = ts.get("location", {})
            t_coords = t_loc.get("coordinates", [None, None]) if isinstance(t_loc, dict) else [None, None]
            t_lon = t_coords[0] if len(t_coords) > 0 else None
            t_lat = t_coords[1] if len(t_coords) > 1 else None
            if t_lat is not None and t_lon is not None:
                trail.append({
                    "lat": t_lat,
                    "lon": t_lon,
                    "timestamp": ts.get("@timestamp"),
                })

        vessels.append({
            "mmsi": src.get("MMSI"),
            "name": src.get("Name"),
            "flag": src.get("Flag"),
            "lat": lat,
            "lon": lon,
            "heading": src.get("Course"),
            "speed": src.get("Speed"),
            "timestamp": src.get("@timestamp"),
            "destination": src.get("Destination"),
            "imo": src.get("IMO"),
            "status": src.get("Status"),
            "vessel_type": src.get("Type"),
            "length": src.get("Length"),
            "eta": src.get("ETA"),
            "trail": trail,
        })

    return vessels


async def get_vessel_identity(es, mmsi: str) -> dict[str, Any] | None:
    """Return the latest AIS record for a single MMSI (for vessel overview)."""
    if es is None:
        return None

    body = {
        "size": 1,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"MMSI.keyword": mmsi}},
                ]
            }
        },
        "sort": [{"@timestamp": "desc"}],
        "_source": [
            "MMSI", "Name", "Flag", "Course", "Speed", "location",
            "@timestamp", "DTG", "Destination", "IMO", "Status", "Type",
            "Length", "ETA",
        ],
    }

    try:
        resp = await es.search(index=ELASTICSEARCH.ais_index, body=body)
    except Exception:
        return None

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return None

    src = hits[0].get("_source", {})
    location = src.get("location", {})
    coords = location.get("coordinates", [None, None]) if isinstance(location, dict) else [None, None]

    return {
        "mmsi": src.get("MMSI"),
        "imo": src.get("IMO"),
        "name": src.get("Name"),
        "flag": src.get("Flag"),
        "vessel_type": src.get("Type"),
        "length": src.get("Length"),
        "destination": src.get("Destination"),
        "speed": src.get("Speed"),
        "heading": src.get("Course"),
        "status": src.get("Status"),
        "eta": src.get("ETA"),
        "lat": coords[1] if len(coords) > 1 else None,
        "lon": coords[0] if len(coords) > 0 else None,
        "last_seen": src.get("@timestamp"),
    }


async def get_index_stats(es, index: str) -> dict[str, Any]:
    """Return doc count, last record timestamp, and 24h hourly histogram for a feed."""
    if es is None:
        return {"status": "down", "total_count": None, "last_record": None, "history": []}

    try:
        count_resp = await es.count(index=index)
        total = count_resp.get("count", 0)
    except Exception:
        return {"status": "down", "total_count": None, "last_record": None, "history": []}

    # Last record
    try:
        last_resp = await es.search(
            index=index,
            body={"size": 1, "sort": [{"@timestamp": "desc"}], "_source": ["@timestamp"]},
        )
        last_hits = last_resp.get("hits", {}).get("hits", [])
        last_record = last_hits[0]["_source"]["@timestamp"] if last_hits else None
    except Exception:
        last_record = None

    # 24h histogram
    history = []
    try:
        hist_resp = await es.search(
            index=index,
            body={
                "size": 0,
                "query": {"range": {"@timestamp": {"gte": "now-24h"}}},
                "aggs": {
                    "hourly": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "1h",
                        }
                    }
                },
            },
        )
        for bucket in hist_resp.get("aggregations", {}).get("hourly", {}).get("buckets", []):
            history.append({
                "timestamp": bucket.get("key_as_string"),
                "count": bucket.get("doc_count", 0),
            })
    except Exception:
        pass

    return {
        "status": "up",
        "total_count": total,
        "last_record": last_record,
        "history": history,
    }


async def get_vessel_track(es, mmsi: str, hours: int = 120) -> list[dict[str, Any]]:
    """Return position history for a single vessel (for map track)."""
    if es is None:
        return []

    body = {
        "size": 500,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"MMSI.keyword": mmsi}},
                    {"range": {"@timestamp": {"gte": f"now-{hours}h"}}},
                ]
            }
        },
        "sort": [{"@timestamp": "asc"}],
        "_source": ["location", "@timestamp", "Speed", "Course"],
    }

    try:
        resp = await es.search(index=ELASTICSEARCH.ais_index, body=body)
    except Exception:
        return []

    points = []
    for hit in resp.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        location = src.get("location", {})
        coords = location.get("coordinates", [None, None]) if isinstance(location, dict) else [None, None]
        if coords[0] is not None and coords[1] is not None:
            points.append({
                "lat": coords[1],
                "lon": coords[0],
                "timestamp": src.get("@timestamp"),
                "speed": src.get("Speed"),
                "heading": src.get("Course"),
            })

    return points
