"""Senzing v3 API client and Cytoscape graph transformation helpers.

v3 endpoints used:
  GET  /entities?attrs={"MMSI_NUMBER":"<mmsi>"}   → search by attributes
  GET  /entities/entity-networks?entities=...      → entity network graph

v3 responses use camelCase keys and search results are flat entity objects
(no RESOLVED_ENTITY wrapper):
  {"entityId": 123, "entityName": "...", "recordSummaries": [...],
   "relationshipData": [...]}
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


# ── client ───────────────────────────────────────────────────────────────

class SenzingClient:
    """Talks to a Senzing v3 REST API."""

    def __init__(self) -> None:
        self.config = SENZING

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def search_by_mmsi(self, mmsi: str) -> list[dict[str, Any]]:
        """Search entities by MMSI attribute.
        Returns the full searchResults list — each item is a flat entity
        with entityId, entityName, recordSummaries, relationshipData, etc."""
        attrs = json.dumps({"MMSI_NUMBER": mmsi})
        resp = self.request(self.config.api_url, params={"attrs": attrs})

        data = resp.get("data")
        if not isinstance(data, dict):
            raise SenzingError("Senzing response missing 'data' object.")
        results = data.get("searchResults")
        if not isinstance(results, list) or not results:
            raise SenzingError(f"No Senzing entity found for MMSI {mmsi}")
        return results

    def get_entity_network(
        self,
        entity_ids: list[int | str],
        max_degrees: int = 2,
    ) -> dict[str, Any]:
        """Fetch the entity network for one or more entity IDs.
        Returns the data dict with entities and entityPaths."""
        entities_param = json.dumps({
            "ENTITIES": [{"ENTITY_ID": int(eid)} for eid in entity_ids],
        })
        url = f"{self.config.api_url}/entity-networks"
        resp = self.request(url, params={
            "entities": entities_param,
            "maxDegrees": str(max_degrees),
        })
        data = resp.get("data")
        if not isinstance(data, dict):
            return {}
        return data

    def request(
        self, url: str, params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            raise SenzingError("Senzing integration is not configured.")

        headers: dict[str, str] = {"Accept": "application/json"}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"

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

def build_network_graph(
    payload: dict[str, Any],
    *,
    focus_entity_id: int | None,
    focus_label: str | None = None,
    focus_type: str | None = None,
) -> CytoscapeGraph:
    """Build a Cytoscape graph from a v3 entity-networks response.

    Payload shape: {entities: [...], entityPaths: [...]}

    `focus_label` / `focus_type` override the label and node type for the
    focus entity (matched by `focus_entity_id`); useful when the caller wants
    to display, e.g., an MMSI rather than the resolved entity name.
    """
    entities = payload.get("entities") or payload.get("ENTITIES") or []
    if not isinstance(entities, list):
        entities = []
    entity_paths = payload.get("entityPaths") or payload.get("ENTITY_PATHS") or []
    if not isinstance(entity_paths, list):
        entity_paths = []

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}

    for entity in entities:
        if not isinstance(entity, dict):
            continue

        eid = entity.get("entityId") or entity.get("ENTITY_ID")
        if eid is None:
            continue
        eid = int(eid)
        is_focus = eid == focus_entity_id

        # --- label ---
        if is_focus and focus_label:
            label = focus_label
        else:
            label = (
                entity.get("entityName")
                or entity.get("ENTITY_NAME")
                or entity.get("bestName")
                or entity.get("BEST_NAME")
                or f"Entity {eid}"
            )

        # --- entity type ---
        if is_focus and focus_type:
            node_type = focus_type
        else:
            record_type = str(entity.get("recordType") or entity.get("RECORD_TYPE") or "").upper()
            label_str = str(label)
            if "VESSEL" in record_type or "MMSI" in label_str.upper():
                node_type = "vessel"
            elif record_type == "ORGANIZATION":
                node_type = "company"
            elif record_type == "PERSON":
                node_type = "person"
            else:
                node_type = "entity"

        # --- record summaries ---
        summaries = entity.get("recordSummaries") or entity.get("RECORD_SUMMARY") or []
        if not isinstance(summaries, list):
            summaries = []

        node_id = f"entity:{eid}"
        nodes[node_id] = {
            "data": {
                "id": node_id,
                "entity_id": eid,
                "label": label,
                "type": node_type,
                "record_count": sum(
                    (s.get("recordCount") or s.get("RECORD_COUNT") or 0)
                    for s in summaries if isinstance(s, dict)
                ),
                "data_sources": [
                    s.get("dataSource") or s.get("DATA_SOURCE")
                    for s in summaries if isinstance(s, dict)
                ],
                "highlighted": is_focus,
            }
        }

        # --- relationships ---
        relationships = entity.get("relationshipData") or entity.get("RELATED_ENTITIES") or []
        if not isinstance(relationships, list):
            relationships = []

        for rel in relationships:
            rel_eid = rel.get("entityId") or rel.get("ENTITY_ID")
            if rel_eid is None:
                continue
            rel_eid = int(rel_eid)

            rel_node_id = f"entity:{rel_eid}"
            if rel_node_id not in nodes:
                rel_label = (
                    rel.get("entityName")
                    or rel.get("ENTITY_NAME")
                    or rel.get("bestName")
                    or rel.get("BEST_NAME")
                    or f"Entity {rel_eid}"
                )
                rel_record_type = str(rel.get("recordType") or rel.get("RECORD_TYPE") or "").upper()
                rel_name_str = str(rel_label)
                if "VESSEL" in rel_record_type or "MMSI" in rel_name_str.upper():
                    rel_type = "vessel"
                elif rel_record_type == "ORGANIZATION":
                    rel_type = "company"
                elif rel_record_type == "PERSON":
                    rel_type = "person"
                else:
                    rel_type = "entity"
                rel_summaries = rel.get("recordSummaries") or rel.get("RECORD_SUMMARY") or []
                if not isinstance(rel_summaries, list):
                    rel_summaries = []
                nodes[rel_node_id] = {
                    "data": {
                        "id": rel_node_id,
                        "entity_id": rel_eid,
                        "label": rel_label,
                        "type": rel_type,
                        "record_count": sum(
                            (s.get("recordCount") or s.get("RECORD_COUNT") or 0)
                            for s in rel_summaries if isinstance(s, dict)
                        ),
                        "data_sources": [
                            s.get("dataSource") or s.get("DATA_SOURCE")
                            for s in rel_summaries if isinstance(s, dict)
                        ],
                        "highlighted": False,
                    }
                }

            match_level_code = (
                rel.get("matchLevelCode") or rel.get("MATCH_LEVEL_CODE") or "RELATED"
            )
            match_key = rel.get("matchKey") or rel.get("MATCH_KEY")
            edge_label = match_key or match_level_code.replace("_", " ").lower()
            ordered = sorted([str(eid), str(rel_eid)])
            edge_id = f"edge:{ordered[0]}:{ordered[1]}:{match_level_code}"
            edges[edge_id] = {
                "data": {
                    "id": edge_id,
                    "source": f"entity:{eid}",
                    "target": f"entity:{rel_eid}",
                    "label": edge_label,
                    "match_level": rel.get("matchLevel") or rel.get("MATCH_LEVEL"),
                    "match_level_code": match_level_code,
                    "is_disclosed": rel.get("isDisclosed") or rel.get("IS_DISCLOSED") or 0,
                    "is_ambiguous": rel.get("isAmbiguous") or rel.get("IS_AMBIGUOUS") or 0,
                }
            }

    # --- entity paths (dashed edges between path members) ---
    for path in entity_paths:
        path_ids = path.get("entities") or path.get("ENTITIES") or []
        if not isinstance(path_ids, list):
            continue
        for i in range(len(path_ids) - 1):
            src, tgt = path_ids[i], path_ids[i + 1]
            ordered = sorted([str(src), str(tgt)])
            edge_id = f"edge:{ordered[0]}:{ordered[1]}:NETWORK_PATH"
            edges.setdefault(edge_id, {
                "data": {
                    "id": edge_id,
                    "source": f"entity:{src}",
                    "target": f"entity:{tgt}",
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
