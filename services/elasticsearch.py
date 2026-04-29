"""Elasticsearch service for AIS position queries.

Uses the async elasticsearch-py client stored on app.state.es.
Auth (basic_auth), verify_certs=False, and ssl_show_warn=False are
configured in app.py startup.

AIS index schema (fields are mostly text/strings):
    @timestamp, Course, Destination, DTG, ETA, Flag, IMO, Length,
    location: {"lon": -1, "lat": 3},
    MMSI, Name, Speed, Status, Type

MMSI is a text field — collapse is not supported, so we use a terms
aggregation with top_hits instead.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from config import ELASTICSEARCH

# All ES queries use a 60s timeout to handle slow/large indices.
_REQUEST_TIMEOUT = 60


class ElasticsearchError(RuntimeError):
    """Raised when an Elasticsearch request or response is invalid."""


def _parse_location(location):
    """Extract lat/lon from location dict {"lon": ..., "lat": ...}."""
    if not isinstance(location, dict):
        return None, None
    lon = location.get("lon")
    lat = location.get("lat")
    return lat, lon


def _to_float(val):
    """Safely cast a value to float (ES fields may be strings)."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


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
        resp = await es.search(
            index=ELASTICSEARCH.ais_index, body=body,
            request_timeout=_REQUEST_TIMEOUT,
        )
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

        lat, lon = _parse_location(src.get("location"))

        # Build trail from trail agg
        trail_hits = bucket.get("trail", {}).get("hits", {}).get("hits", [])
        trail = []
        for th in trail_hits:
            ts = th.get("_source", {})
            t_lat, t_lon = _parse_location(ts.get("location"))
            if t_lat is not None and t_lon is not None:
                trail.append({
                    "lat": _to_float(t_lat),
                    "lon": _to_float(t_lon),
                    "timestamp": ts.get("@timestamp"),
                })

        vessels.append({
            "mmsi": src.get("MMSI"),
            "name": src.get("Name"),
            "flag": src.get("Flag"),
            "lat": _to_float(lat),
            "lon": _to_float(lon),
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
        resp = await es.search(
            index=ELASTICSEARCH.ais_index, body=body,
            request_timeout=_REQUEST_TIMEOUT,
        )
    except Exception:
        return None

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return None

    src = hits[0].get("_source", {})
    lat, lon = _parse_location(src.get("location"))

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
        "lat": _to_float(lat),
        "lon": _to_float(lon),
        "last_seen": src.get("@timestamp"),
    }


async def get_index_stats(es, index: str) -> dict[str, Any]:
    """Return doc count, last record timestamp, and 24h hourly histogram for a feed."""
    if es is None:
        return {"status": "down", "total_count": None, "last_record": None, "history": []}

    try:
        count_resp = await es.count(index=index, request_timeout=_REQUEST_TIMEOUT)
        total = count_resp.get("count", 0)
    except Exception:
        return {"status": "down", "total_count": None, "last_record": None, "history": []}

    # Last record
    try:
        last_resp = await es.search(
            index=index,
            body={"size": 1, "sort": [{"@timestamp": "desc"}], "_source": ["@timestamp"]},
            request_timeout=_REQUEST_TIMEOUT,
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
            request_timeout=_REQUEST_TIMEOUT,
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
        resp = await es.search(
            index=ELASTICSEARCH.ais_index, body=body,
            request_timeout=_REQUEST_TIMEOUT,
        )
    except Exception:
        return []

    points = []
    for hit in resp.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        lat, lon = _parse_location(src.get("location"))
        lat = _to_float(lat)
        lon = _to_float(lon)
        if lat is not None and lon is not None:
            points.append({
                "lat": lat,
                "lon": lon,
                "timestamp": src.get("@timestamp"),
                "speed": src.get("Speed"),
                "heading": src.get("Course"),
            })

    return points


async def get_vessel_pattern(
    es, mmsi: str, days: int = 365,
) -> dict[str, Any]:
    """Return a 7×24 AIS message count matrix (day-of-week × hour-of-day).

    Fetches @timestamp values via PIT-based pagination so the full date range
    is consumed regardless of index size.  Returns::

        {
            "x_labels": ["0", "1", ..., "23"],
            "y_labels": ["Mon", ..., "Sun"],
            "values":   [[int, ...], ...],   # shape 7×24
        }

    Returns empty labels/values when ES is unavailable or no data exists.
    """
    _empty: dict[str, Any] = {"x_labels": [], "y_labels": [], "values": []}
    if es is None:
        return _empty

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    query = {
        "bool": {
            "filter": [
                {"term": {"MMSI.keyword": mmsi}},
                {"range": {"@timestamp": {
                    "gte": start_dt.isoformat(),
                    "lt": end_dt.isoformat(),
                }}},
            ]
        }
    }

    timestamps: list[datetime] = []
    pit_id: str | None = None
    try:
        pit_resp = await es.open_point_in_time(
            index=ELASTICSEARCH.ais_index,
            params={"keep_alive": "2m"},
        )
        pit_id = pit_resp["id"]

        search_after = None
        while True:
            body: dict = {
                "size": 1000,
                "query": query,
                "_source": ["@timestamp"],
                "sort": [{"@timestamp": "asc"}, {"_shard_doc": "asc"}],
                "pit": {"id": pit_id, "keep_alive": "2m"},
            }
            if search_after is not None:
                body["search_after"] = search_after

            resp = await es.search(body=body, request_timeout=_REQUEST_TIMEOUT)
            pit_id = resp.get("pit_id", pit_id)
            hits = resp.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                ts_str = hit.get("_source", {}).get("@timestamp")
                if ts_str:
                    try:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        timestamps.append(ts)
                    except ValueError:
                        pass

            search_after = hits[-1]["sort"]
            if len(hits) < 1000:
                break
    except Exception:
        return _empty
    finally:
        if pit_id:
            try:
                await es.close_point_in_time(body={"id": pit_id})
            except Exception:
                pass

    if not timestamps:
        return _empty

    matrix = [[0] * 24 for _ in range(7)]
    for ts in timestamps:
        matrix[ts.weekday()][ts.hour] += 1

    return {
        "x_labels": [str(h) for h in range(24)],
        "y_labels": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        "values": matrix,
    }
