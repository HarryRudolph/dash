"""Application configuration loaded from environment variables."""

import os
from dataclasses import dataclass


def _bool(value: str) -> bool:
    return value.lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class SensorConfig:
    name: str
    lat: float
    lon: float

    def as_dict(self) -> dict[str, float | str]:
        return {
            "name": self.name,
            "lat": self.lat,
            "lon": self.lon,
        }


@dataclass(frozen=True)
class TileServerConfig:
    url: str
    credit: str
    maximum_level: int

    @property
    def satvis_layer(self) -> str:
        return "Custom" if self.url else "OfflineHighres"

    def as_dict(self) -> dict[str, int | str]:
        return {
            "url": self.url,
            "credit": self.credit,
            "maximumLevel": self.maximum_level,
        }


@dataclass(frozen=True)
class SenzingConfig:
    api_url: str
    data_source: str
    auth_token: str
    timeout_seconds: float
    entity_by_record_path: str
    entity_by_entity_path: str

    @property
    def enabled(self) -> bool:
        return bool(self.api_url and self.data_source)


@dataclass(frozen=True)
class RuntimeConfig:
    offline_mode: bool


DEV_MODE = _bool(os.environ.get("DASHBOARD_DEV", ""))

SENSOR = SensorConfig(
    name=os.environ.get("SENSOR_NAME", "Sensor Alpha"),
    lat=float(os.environ.get("SENSOR_LAT", "51.5074")),
    lon=float(os.environ.get("SENSOR_LON", "-0.1278")),
)

TILE_SERVER = TileServerConfig(
    url=os.environ.get("TILE_SERVER_URL", ""),
    credit=os.environ.get("TILE_SERVER_CREDIT", ""),
    maximum_level=int(os.environ.get("TILE_SERVER_MAX_ZOOM", "18")),
)

SENZING = SenzingConfig(
    api_url=os.environ.get("SZ_API_URL", "").rstrip("/"),
    data_source=os.environ.get("SZ_DATA_SOURCE", "AIS"),
    auth_token=os.environ.get("SZ_API_TOKEN", ""),
    timeout_seconds=float(os.environ.get("SZ_TIMEOUT_SECONDS", "10")),
    entity_by_record_path=os.environ.get(
        "SZ_ENTITY_BY_RECORD_PATH",
        "/entity-resolution/v1/entities/by-record/{data_source}/{record_id}",
    ),
    entity_by_entity_path=os.environ.get(
        "SZ_ENTITY_BY_ENTITY_PATH",
        "/entity-resolution/v1/entities/{entity_id}",
    ),
)

RUNTIME = RuntimeConfig(
    offline_mode=_bool(os.environ.get("DASHBOARD_OFFLINE_MODE", "")),
)
