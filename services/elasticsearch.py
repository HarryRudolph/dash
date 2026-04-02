"""Thin Elasticsearch HTTP client for AIS position queries."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from config import ELASTICSEARCH


class ElasticsearchError(RuntimeError):
    """Raised when an Elasticsearch request or response is invalid."""


class ElasticsearchClient:
    """Fetches AIS position data from Elasticsearch over HTTP."""

    def __init__(self) -> None:
        self.config = ELASTICSEARCH

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def get_latest_positions(self, mmsis: list[str]) -> list[dict[str, Any]]:
        """Return the latest position and a 5-point trail for each MMSI."""
        if not self.enabled:
            raise ElasticsearchError("Elasticsearch is not configured.")

        body = {
            "size": len(mmsis),
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"mmsi": mmsis}},
                    ]
                }
            },
            "sort": [{"@timestamp": "desc"}],
            "collapse": {
                "field": "mmsi",
                "inner_hits": {
                    "name": "trail",
                    "size": 5,
                    "sort": [{"@timestamp": "desc"}],
                    "_source": ["lat", "lon", "@timestamp"],
                },
            },
            "_source": [
                "mmsi", "name", "flag", "lat", "lon",
                "heading", "speed_over_ground", "@timestamp",
            ],
        }

        hits = self._search(body)
        vessels: list[dict[str, Any]] = []
        for hit in hits:
            src = hit.get("_source", {})
            trail_hits = (
                hit.get("inner_hits", {})
                .get("trail", {})
                .get("hits", {})
                .get("hits", [])
            )
            trail = [
                {
                    "lat": th["_source"]["lat"],
                    "lon": th["_source"]["lon"],
                    "timestamp": th["_source"].get("@timestamp"),
                }
                for th in trail_hits
                if "_source" in th
            ]
            vessels.append({
                "mmsi": src.get("mmsi"),
                "name": src.get("name"),
                "flag": src.get("flag"),
                "lat": src.get("lat"),
                "lon": src.get("lon"),
                "heading": src.get("heading"),
                "speed": src.get("speed_over_ground"),
                "timestamp": src.get("@timestamp"),
                "trail": trail,
            })
        return vessels

    def _search(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        url = f"{self.config.url}/{self.config.ais_index}/_search"
        data = json.dumps(body).encode("utf-8")
        request = Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=10) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ElasticsearchError(
                f"Elasticsearch request failed with HTTP {exc.code}: {detail or exc.reason}"
            ) from exc
        except URLError as exc:
            raise ElasticsearchError(
                f"Elasticsearch request failed: {exc.reason}"
            ) from exc

        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ElasticsearchError(
                "Elasticsearch response was not valid JSON."
            ) from exc

        return parsed.get("hits", {}).get("hits", [])
