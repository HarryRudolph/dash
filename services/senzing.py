"""Senzing v3 API client and Cytoscape graph transformation helpers.

`SZ_API_URL` is the API base URL (e.g. `https://server/api`), without any
endpoint suffix. v3 endpoints used:
  GET  {base}/entities?attrs={"MMSI_NUMBER":"<mmsi>"}   → search by attributes
  GET  {base}/entity-networks?entities=<id>,<id>        → entity network graph

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
        url = f"{self.config.api_url}/entities"
        resp = self.request(url, params={"attrs": attrs})

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
        Returns the data dict with entities and entityPaths.

        v3 REST expects `entities` as a comma-separated list of identifiers,
        not the v2 JSON-wrapped {"ENTITIES":[...]} payload — sending JSON
        causes G2 to read the embedded `:` as a DATA_SOURCE/RECORD_ID
        separator and emit `0027E|Unknown DATA_SOURCE value`.
        """
        entities_param = ",".join(str(int(eid)) for eid in entity_ids)
        url = f"{self.config.api_url}/entity-networks"
        resp = self.request(url, params={
            "entities": entities_param,
            "maxDegrees": str(max_degrees),
            # featureMode=NONE (the default) strips the `features` map, which
            # leaves no signal for classifying node type. REPRESENTATIVE keeps
            # one canonical value per feature type.
            "featureMode": "REPRESENTATIVE",
            "forceMinimal": "false",
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

def _entity_core(obj: dict[str, Any]) -> dict[str, Any]:
    """Unwrap the v3 `resolvedEntity` envelope when present."""
    inner = obj.get("resolvedEntity")
    return inner if isinstance(inner, dict) else obj


_VESSEL_FEATURE_KEYS = ("MMSI_NUMBER", "MMSI", "IMO_NUMBER", "IMO", "LR_NUMBER", "VESSEL_NAME")
_PERSON_NAME_TYPES = {"NAME_FIRST", "NAME_LAST", "NAME_FULL", "NAME_PERSON"}


def _classify_node_type(core: dict[str, Any], label: str) -> str:
    """Classify an entity as vessel / company / person / entity.

    v3 entity-networks doesn't expose a top-level `recordType`; the reliable
    signal is the `features` map (only populated when the request asks for
    `featureMode=REPRESENTATIVE` or richer). Falls back to `recordType` /
    label heuristics so the classifier still produces something useful for
    minimal payloads.
    """
    features = core.get("features") or core.get("FEATURES") or {}
    if isinstance(features, dict):
        for key in _VESSEL_FEATURE_KEYS:
            if features.get(key):
                return "vessel"
        names = features.get("NAME") or features.get("NAMES") or []
        if isinstance(names, list):
            for n in names:
                if not isinstance(n, dict):
                    continue
                ftype = str(
                    n.get("featureType")
                    or n.get("FEATURE_TYPE")
                    or n.get("usageType")
                    or n.get("USAGE_TYPE")
                    or ""
                ).upper()
                if ftype == "NAME_ORG":
                    return "company"
                if ftype in _PERSON_NAME_TYPES:
                    return "person"

    record_type = str(core.get("recordType") or core.get("RECORD_TYPE") or "").upper()
    if "VESSEL" in record_type or "MMSI" in label.upper():
        return "vessel"
    if record_type == "ORGANIZATION":
        return "company"
    if record_type == "PERSON":
        return "person"
    return "entity"


def _node_data(
    obj: dict[str, Any],
    *,
    eid: int,
    is_focus: bool,
    label_override: str | None = None,
    type_override: str | None = None,
) -> dict[str, Any]:
    core = _entity_core(obj)
    label = label_override or (
        core.get("entityName")
        or core.get("ENTITY_NAME")
        or core.get("bestName")
        or core.get("BEST_NAME")
        or f"Entity {eid}"
    )
    node_type = type_override or _classify_node_type(core, str(label))
    summaries = core.get("recordSummaries") or core.get("RECORD_SUMMARY") or []
    if not isinstance(summaries, list):
        summaries = []
    return {
        "id": f"entity:{eid}",
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


def build_network_graph(
    payload: dict[str, Any],
    *,
    focus_entity_id: int | None,
    focus_label: str | None = None,
    focus_type: str | None = None,
) -> CytoscapeGraph:
    """Build a Cytoscape graph from a v3 entity-networks response.

    Payload shape:
      {entities: [{resolvedEntity: {entityId, entityName, ...},
                   relatedEntities: [{entityId, entityName,
                                      matchInfo: {matchKey, ...}}]}],
       entityPaths: [...]}

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

        core = _entity_core(entity)
        eid = core.get("entityId") or core.get("ENTITY_ID")
        if eid is None:
            continue
        eid = int(eid)
        is_focus = eid == focus_entity_id

        node_id = f"entity:{eid}"
        nodes[node_id] = {
            "data": _node_data(
                entity,
                eid=eid,
                is_focus=is_focus,
                label_override=focus_label if is_focus else None,
                type_override=focus_type if is_focus else None,
            ),
        }

        relationships = (
            entity.get("relatedEntities")
            or entity.get("RELATED_ENTITIES")
            or entity.get("relationshipData")
            or []
        )
        if not isinstance(relationships, list):
            relationships = []

        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            rel_core = _entity_core(rel)
            rel_eid = rel_core.get("entityId") or rel_core.get("ENTITY_ID")
            if rel_eid is None:
                continue
            rel_eid = int(rel_eid)

            rel_node_id = f"entity:{rel_eid}"
            if rel_node_id not in nodes:
                nodes[rel_node_id] = {
                    "data": _node_data(rel, eid=rel_eid, is_focus=False),
                }

            # match metadata is nested under `matchInfo` in v3, flat in older
            match_info = rel.get("matchInfo") if isinstance(rel.get("matchInfo"), dict) else rel
            match_level_code = (
                match_info.get("matchLevelCode")
                or match_info.get("MATCH_LEVEL_CODE")
                or "RELATED"
            )
            match_key = match_info.get("matchKey") or match_info.get("MATCH_KEY")
            edge_label = match_key or match_level_code.replace("_", " ").lower()
            ordered = sorted([str(eid), str(rel_eid)])
            edge_id = f"edge:{ordered[0]}:{ordered[1]}:{match_level_code}"
            edges[edge_id] = {
                "data": {
                    "id": edge_id,
                    "source": f"entity:{eid}",
                    "target": f"entity:{rel_eid}",
                    "label": edge_label,
                    "match_level": match_info.get("matchLevel") or match_info.get("MATCH_LEVEL"),
                    "match_level_code": match_level_code,
                    "is_disclosed": match_info.get("isDisclosed") or match_info.get("IS_DISCLOSED") or 0,
                    "is_ambiguous": match_info.get("isAmbiguous") or match_info.get("IS_AMBIGUOUS") or 0,
                },
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
