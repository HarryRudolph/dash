"""Senzing v3 API client and Cytoscape graph transformation helpers.

v3 endpoints used:
  GET  /entities?attrs={"MMSI_NUMBER":"<mmsi>"}   → search by attributes
  GET  /entities/entity-networks?entities=...      → entity network graph
"""

from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass
from typing import Any

import requests

from config import SENZING


class SenzingError(RuntimeError):
    """Raised when the Senzing API request or response is invalid."""


@dataclass(frozen=True)
class CytoscapeGraph:
    elements: dict[str, list[dict[str, Any]]]
    meta: dict[str, Any]


# ── helpers ──────────────────────────────────────────────────────────────

def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


# ── client ───────────────────────────────────────────────────────────────

class SenzingClient:
    """Talks to a Senzing v3 REST API."""

    def __init__(self) -> None:
        self.config = SENZING

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    # -- public API -------------------------------------------------------

    def search_by_mmsi(self, mmsi: str) -> dict[str, Any]:
        """Search entities by MMSI attribute.
        Returns the top search result dict which contains
        RESOLVED_ENTITY and RELATED_ENTITIES."""
        attrs = json.dumps({"MMSI_NUMBER": mmsi})
        resp = self._get(self.config.api_url, params={"attrs": attrs})

        results = _as_list(
            _as_dict(resp.get("data")).get("searchResults")
        )
        if not results:
            raise SenzingError(f"No Senzing entity found for MMSI {mmsi}")
        return results[0]

    def get_entity_network(
        self,
        entity_ids: list[int | str],
        max_degrees: int = 2,
    ) -> dict[str, Any]:
        """Fetch the entity network for one or more entity IDs.
        Returns the data dict with ENTITIES and ENTITY_PATHS."""
        entities_param = json.dumps({
            "ENTITIES": [{"ENTITY_ID": int(eid)} for eid in entity_ids],
        })
        url = f"{self.config.api_url}/entity-networks"
        resp = self._get(url, params={
            "entities": entities_param,
            "maxDegrees": str(max_degrees),
        })
        return _as_dict(resp.get("data"))

    # -- transport --------------------------------------------------------

    def _get(
        self, url: str, params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            raise SenzingError("Senzing integration is not configured.")

        headers: dict[str, str] = {"Accept": "application/json"}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"

        # Build URL with encoded query string (matches the pattern the user
        # validated: urllib.parse.urlencode → requests.get)
        qs = urllib.parse.urlencode(params or {})
        full_url = f"{url}?{qs}" if qs else url

        try:
            resp = requests.get(
                full_url,
                headers=headers,
                timeout=self.config.timeout_seconds,
                verify=self.config.verify_ssl,
            )
            resp.raise_for_status()
        except requests.ConnectionError as exc:
            raise SenzingError(f"Senzing connection failed: {exc}") from exc
        except requests.HTTPError as exc:
            detail = ""
            try:
                detail = exc.response.text
            except Exception:
                pass
            raise SenzingError(
                f"Senzing request failed with HTTP {exc.response.status_code}: {detail}"
            ) from exc
        except requests.Timeout as exc:
            raise SenzingError("Senzing request timed out") from exc

        try:
            parsed = resp.json()
        except ValueError as exc:
            raise SenzingError("Senzing response was not valid JSON.") from exc

        if not isinstance(parsed, dict):
            raise SenzingError("Senzing response JSON must be an object.")
        return parsed


# ── cytoscape graph builder ──────────────────────────────────────────────

def build_cytoscape_graph(
    payload: dict[str, Any],
    *,
    focus_entity_id: int | None,
    focus_label: str | None = None,
    focus_type: str | None = None,
) -> CytoscapeGraph:
    """Convert a Senzing v3 response into Cytoscape-compatible elements.

    Handles both shapes:
      - Search result: {RESOLVED_ENTITY: {...}, RELATED_ENTITIES: [...]}
      - Entity network: {ENTITIES: [...], ENTITY_PATHS: [...]}
    """
    entities = payload.get("ENTITIES")
    if isinstance(entities, list):
        return _build_network_graph(
            entities,
            focus_entity_id=focus_entity_id,
            focus_label=focus_label,
            focus_type=focus_type,
            entity_paths=_as_list(payload.get("ENTITY_PATHS")),
        )

    return _build_entity_graph(
        payload,
        focus_entity_id=focus_entity_id,
        focus_label=focus_label,
        focus_type=focus_type,
    )


# ── single-entity graph (search result) ─────────────────────────────────

def _build_entity_graph(
    payload: dict[str, Any],
    *,
    focus_entity_id: int | None,
    focus_label: str | None,
    focus_type: str | None,
) -> CytoscapeGraph:
    resolved_entity = _resolved_entity(payload)
    if not resolved_entity:
        raise SenzingError("Senzing response did not include a resolved entity.")

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}

    root_node = _node_from_entity(
        resolved_entity,
        node_type=focus_type or _infer_entity_type(resolved_entity),
        focus_label=focus_label,
        highlighted=True,
    )
    nodes[root_node["data"]["id"]] = root_node

    root_entity_id = resolved_entity.get("ENTITY_ID")

    for related in _related_entities(payload):
        related_entity_id = related.get("ENTITY_ID")
        if related_entity_id is None:
            continue
        node = _node_from_entity(related)
        nodes[node["data"]["id"]] = node
        edge = _edge_from_related(root_entity_id, related)
        edges[edge["data"]["id"]] = edge

    return CytoscapeGraph(
        elements={
            "nodes": list(nodes.values()),
            "edges": list(edges.values()),
        },
        meta={
            "focus_entity_id": int(root_entity_id) if root_entity_id else None,
            "related_count": len(edges),
            "path_count": 0,
        },
    )


# ── multi-entity network graph ──────────────────────────────────────────

def _build_network_graph(
    entities: list[dict[str, Any]],
    *,
    focus_entity_id: int | None,
    focus_label: str | None,
    focus_type: str | None,
    entity_paths: list[dict[str, Any]],
) -> CytoscapeGraph:
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}

    for entity_payload in entities:
        resolved_entity = _resolved_entity(entity_payload)
        if not resolved_entity:
            continue

        entity_id = resolved_entity.get("ENTITY_ID")
        node = _node_from_entity(
            resolved_entity,
            node_type=focus_type if entity_id == focus_entity_id else None,
            focus_label=focus_label if entity_id == focus_entity_id else None,
            highlighted=entity_id == focus_entity_id,
        )
        nodes[node["data"]["id"]] = node

        for related in _related_entities(entity_payload):
            related_entity_id = related.get("ENTITY_ID")
            if related_entity_id is None:
                continue
            nodes[_node_id(related_entity_id)] = _node_from_entity(related)
            edge = _edge_from_related(entity_id, related)
            edges[edge["data"]["id"]] = edge

    for path in entity_paths:
        path_entities = _as_list(path.get("ENTITIES"))
        for index in range(len(path_entities) - 1):
            src = path_entities[index]
            tgt = path_entities[index + 1]
            edge_id = _edge_id(src, tgt, "NETWORK_PATH")
            edges.setdefault(edge_id, {
                "data": {
                    "id": edge_id,
                    "source": _node_id(src),
                    "target": _node_id(tgt),
                    "label": "path",
                    "match_level_code": "NETWORK_PATH",
                    "line_style": "dashed",
                },
            })

    return CytoscapeGraph(
        elements={
            "nodes": list(nodes.values()),
            "edges": list(edges.values()),
        },
        meta={
            "focus_entity_id": focus_entity_id,
            "related_count": len(edges),
            "path_count": len(entity_paths),
        },
    )


# ── payload accessors ────────────────────────────────────────────────────

def _resolved_entity(payload: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(payload.get("RESOLVED_ENTITY"))


def _related_entities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return _as_list(payload.get("RELATED_ENTITIES"))


# ── node / edge factories ───────────────────────────────────────────────

def _node_from_entity(
    entity: dict[str, Any],
    *,
    node_type: str | None = None,
    focus_label: str | None = None,
    highlighted: bool = False,
) -> dict[str, Any]:
    entity_id = entity.get("ENTITY_ID")
    if entity_id is None:
        raise SenzingError("Entity node is missing ENTITY_ID.")

    label = _coalesce(
        focus_label,
        entity.get("ENTITY_NAME"),
        entity.get("BEST_NAME"),
        entity.get("RECORD_ID"),
        f"Entity {entity_id}",
    )

    inferred_type = node_type or _infer_entity_type(entity)
    record_summary = _as_list(entity.get("RECORD_SUMMARY"))
    record_count = sum(
        item.get("RECORD_COUNT", 0)
        for item in record_summary
        if isinstance(item, dict)
    )

    return {
        "data": {
            "id": _node_id(entity_id),
            "entity_id": int(entity_id),
            "label": label,
            "type": inferred_type,
            "record_count": record_count,
            "data_sources": [
                item.get("DATA_SOURCE")
                for item in record_summary
                if isinstance(item, dict)
            ],
            "highlighted": highlighted,
        }
    }


def _edge_from_related(
    source_entity_id: int | str, related: dict[str, Any],
) -> dict[str, Any]:
    target_entity_id = related.get("ENTITY_ID")
    if target_entity_id is None:
        raise SenzingError("Related entity is missing ENTITY_ID.")

    match_level_code = related.get("MATCH_LEVEL_CODE", "RELATED")
    label = related.get("MATCH_KEY") or match_level_code.replace("_", " ").lower()

    return {
        "data": {
            "id": _edge_id(source_entity_id, target_entity_id, match_level_code),
            "source": _node_id(source_entity_id),
            "target": _node_id(target_entity_id),
            "label": label,
            "match_level": related.get("MATCH_LEVEL"),
            "match_level_code": match_level_code,
            "is_disclosed": related.get("IS_DISCLOSED", 0),
            "is_ambiguous": related.get("IS_AMBIGUOUS", 0),
        }
    }


def _infer_entity_type(entity: dict[str, Any]) -> str:
    record_type = str(entity.get("RECORD_TYPE") or "").upper()
    label = str(_coalesce(entity.get("ENTITY_NAME"), entity.get("BEST_NAME")) or "")

    if "VESSEL" in record_type or "MMSI" in label.upper():
        return "vessel"
    if record_type == "ORGANIZATION":
        return "company"
    if record_type == "PERSON":
        return "person"
    return "entity"


def _node_id(entity_id: int | str) -> str:
    return f"entity:{entity_id}"


def _edge_id(
    source_entity_id: int | str,
    target_entity_id: int | str,
    relation: str,
) -> str:
    ordered = sorted([str(source_entity_id), str(target_entity_id)])
    return f"edge:{ordered[0]}:{ordered[1]}:{relation}"
