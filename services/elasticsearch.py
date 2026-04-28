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


_OWNERSHIP_ROLES: tuple[tuple[str, str, str], ...] = (
    ("registered_owner", "RegisteredOwner", "RegisteredOwner_NationalityofControl"),
    ("operator", "Operator", "Operator_NationalityofControl"),
    ("group_beneficial_owner", "GroupBeneficialOwner", "GroupBeneficialOwner_NationalityofControl"),
    ("ship_manager", "ShipManager", "ShipManager_NationalityofControl"),
    ("technical_manager", "TechnicalManager", "TechnicalManager_NationalityofControl"),
    ("doc_company", "DOCCompany", "DOC_NationalityofControl"),
)


async def get_vessel_ownership(es, imo: str | int | None) -> dict[str, Any] | None:
    """Return the most recent ownership record for a vessel by IMO/LRNO.

    The ownership index is updated infrequently — the latest hit may be
    many years old, but is still the best available answer.
    """
    if es is None or imo is None:
        return None

    imo_str = str(imo).strip()
    if not imo_str or imo_str == "0":
        return None

    source_fields = ["LRNO", "@timestamp"]
    for _, name_field, country_field in _OWNERSHIP_ROLES:
        source_fields.append(name_field)
        source_fields.append(country_field)

    body = {
        "size": 1,
        "query": {
            "bool": {
                "filter": [{"term": {"LRNO.keyword": imo_str}}]
            }
        },
        "sort": [{"@timestamp": "desc"}],
        "_source": source_fields,
    }

    try:
        resp = await es.search(
            index=ELASTICSEARCH.ownership_index, body=body,
            request_timeout=_REQUEST_TIMEOUT,
        )
    except Exception:
        return None

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return None

    src = hits[0].get("_source", {})

    record: dict[str, Any] = {"as_of": src.get("@timestamp")}
    for out_key, name_field, country_field in _OWNERSHIP_ROLES:
        name = src.get(name_field)
        country = src.get(country_field)
        if name and country:
            record[out_key] = f"{name} ({country})"
        else:
            record[out_key] = name or None
    return record


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
