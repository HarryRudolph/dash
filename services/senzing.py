"""Thin Senzing API client and graph transformation helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen

from config import SENZING


class SenzingError(RuntimeError):
    """Raised when the Senzing API request or response is invalid."""


@dataclass(frozen=True)
class CytoscapeGraph:
    elements: dict[str, list[dict[str, Any]]]
    meta: dict[str, Any]


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


class SenzingClient:
    """Fetches entity data from a Senzing-compatible REST API."""

    def __init__(self) -> None:
        self.config = SENZING

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def get_entity_by_record_id(self, record_id: str) -> dict[str, Any]:
        return self._request(
            self.config.entity_by_record_path,
            data_source=self.config.data_source,
            record_id=record_id,
        )

    def get_entity_by_entity_id(self, entity_id: int | str) -> dict[str, Any]:
        return self._request(
            self.config.entity_by_entity_path,
            entity_id=str(entity_id),
        )

    def _request(self, path_template: str, **params: str) -> dict[str, Any]:
        if not self.enabled:
            raise SenzingError("Senzing integration is not configured.")

        template_values = {
            key: value
            for key, value in params.items()
        }
        template_values.update({
            f"{key}_quoted": quote(value, safe="")
            for key, value in params.items()
        })

        relative_path = path_template.format_map(template_values)
        base_url = f"{self.config.api_url}/"
        url = urljoin(base_url, relative_path.lstrip("/"))

        headers = {"Accept": "application/json"}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"

        request = Request(url, headers=headers, method="GET")

        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SenzingError(
                f"Senzing request failed with HTTP {exc.code}: {detail or exc.reason}"
            ) from exc
        except URLError as exc:
            raise SenzingError(f"Senzing request failed: {exc.reason}") from exc

        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SenzingError("Senzing response was not valid JSON.") from exc

        if not isinstance(parsed, dict):
            raise SenzingError("Senzing response JSON must be an object.")
        return parsed


def build_cytoscape_graph(
    payload: dict[str, Any],
    *,
    focus_entity_id: int | None,
    focus_label: str | None = None,
    focus_type: str | None = None,
) -> CytoscapeGraph:
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


def _build_entity_graph(
    payload: dict[str, Any],
    *,
    focus_entity_id: int | None,
    focus_label: str | None,
    focus_type: str | None,
) -> CytoscapeGraph:
    resolved_entity = _resolved_entity(payload)
    if not resolved_entity:
        raise SenzingError("Senzing entity response did not include a resolved entity.")

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}

    root_node = _node_from_entity(
        resolved_entity,
        node_type=focus_type or _infer_entity_type(resolved_entity),
        focus_label=focus_label,
        highlighted=True,
    )
    nodes[root_node["data"]["id"]] = root_node

    root_entity_id = resolved_entity.get("ENTITY_ID") or resolved_entity.get("entityId")

    for related in _related_entities(payload):
        related_entity_id = related.get("ENTITY_ID") or related.get("entityId")
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
            "focus_entity_id": int(root_entity_id),
            "related_count": len(edges),
            "path_count": 0,
        },
    )


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

        entity_id = resolved_entity.get("ENTITY_ID") or resolved_entity.get("entityId")
        node = _node_from_entity(
            resolved_entity,
            node_type=focus_type if entity_id == focus_entity_id else None,
            focus_label=focus_label if entity_id == focus_entity_id else None,
            highlighted=entity_id == focus_entity_id,
        )
        nodes[node["data"]["id"]] = node

        for related in _related_entities(entity_payload):
            related_entity_id = related.get("ENTITY_ID") or related.get("entityId")
            if related_entity_id is None:
                continue
            nodes[_node_id(related_entity_id)] = _node_from_entity(related)
            edge = _edge_from_related(entity_id, related)
            edges[edge["data"]["id"]] = edge

    for path in entity_paths:
        path_entities = _as_list(path.get("ENTITIES"))
        for index in range(len(path_entities) - 1):
            source_entity_id = path_entities[index]
            target_entity_id = path_entities[index + 1]
            edge_id = _edge_id(source_entity_id, target_entity_id, "NETWORK_PATH")
            edges.setdefault(
                edge_id,
                {
                    "data": {
                        "id": edge_id,
                        "source": _node_id(source_entity_id),
                        "target": _node_id(target_entity_id),
                        "label": "path",
                        "match_level_code": "NETWORK_PATH",
                        "line_style": "dashed",
                    }
                },
            )

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


def _resolved_entity(payload: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(
        _coalesce(payload.get("RESOLVED_ENTITY"), payload.get("resolvedEntity"))
    )


def _related_entities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return _as_list(
        _coalesce(payload.get("RELATED_ENTITIES"), payload.get("relatedEntities"))
    )


def _node_from_entity(
    entity: dict[str, Any],
    *,
    node_type: str | None = None,
    focus_label: str | None = None,
    highlighted: bool = False,
) -> dict[str, Any]:
    entity_id = entity.get("ENTITY_ID") or entity.get("entityId")
    if entity_id is None:
        raise SenzingError("Entity node is missing an entity id.")

    label = _coalesce(
        focus_label,
        entity.get("ENTITY_NAME"),
        entity.get("entityName"),
        entity.get("BEST_NAME"),
        entity.get("bestName"),
        entity.get("RECORD_ID"),
        entity.get("recordId"),
        f"Entity {entity_id}",
    )

    inferred_type = node_type or _infer_entity_type(entity)
    record_summary = _record_summary(entity)
    record_count = sum(
        item.get("RECORD_COUNT", item.get("recordCount", 0))
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
                item.get("DATA_SOURCE") or item.get("dataSource")
                for item in record_summary
                if isinstance(item, dict)
            ],
            "highlighted": highlighted,
        }
    }


def _edge_from_related(source_entity_id: int | str, related: dict[str, Any]) -> dict[str, Any]:
    target_entity_id = related.get("ENTITY_ID") or related.get("entityId")
    if target_entity_id is None:
        raise SenzingError("Related entity is missing an entity id.")

    match_level_code = (
        related.get("MATCH_LEVEL_CODE")
        or related.get("matchLevelCode")
        or "RELATED"
    )
    label = (
        related.get("MATCH_KEY")
        or related.get("matchKey")
        or match_level_code.replace("_", " ").lower()
    )

    return {
        "data": {
            "id": _edge_id(source_entity_id, target_entity_id, match_level_code),
            "source": _node_id(source_entity_id),
            "target": _node_id(target_entity_id),
            "label": label,
            "match_level": related.get("MATCH_LEVEL") or related.get("matchLevel"),
            "match_level_code": match_level_code,
            "is_disclosed": related.get("IS_DISCLOSED") or related.get("isDisclosed", 0),
            "is_ambiguous": related.get("IS_AMBIGUOUS") or related.get("isAmbiguous", 0),
        }
    }


def _record_summary(entity: dict[str, Any]) -> list[dict[str, Any]]:
    return _as_list(_coalesce(entity.get("RECORD_SUMMARY"), entity.get("recordSummaries")))


def _infer_entity_type(entity: dict[str, Any]) -> str:
    record_type = str(
        _coalesce(
            entity.get("RECORD_TYPE"),
            entity.get("recordType"),
        )
        or ""
    ).upper()
    label = str(
        _coalesce(
            entity.get("ENTITY_NAME"),
            entity.get("entityName"),
            entity.get("BEST_NAME"),
            entity.get("bestName"),
        )
        or ""
    )

    if "VESSEL" in record_type or "MMSI" in label.upper():
        return "vessel"
    if record_type == "ORGANIZATION":
        return "company"
    if record_type == "PERSON":
        return "person"
    return "entity"


def _node_id(entity_id: int | str) -> str:
    return f"entity:{entity_id}"


def _edge_id(source_entity_id: int | str, target_entity_id: int | str, relation: str) -> str:
    ordered = sorted([str(source_entity_id), str(target_entity_id)])
    return f"edge:{ordered[0]}:{ordered[1]}:{relation}"
