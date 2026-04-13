"""Application configuration loaded from environment variables."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _bool(value: str) -> bool:
    return value.lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class SensorConfig:
    name: str
    lat: float
    lon: float

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "lat": self.lat,
            "lon": self.lon,
        }


@dataclass(frozen=True)
class SatvisConfig:
    default_tags: str

    def as_dict(self) -> dict:
        return {
            "defaultTags": self.default_tags,
        }


@dataclass(frozen=True)
class TileServerConfig:
    url: str
    credit: str
    maximum_level: int

    @property
    def satvis_layer(self) -> str:
        # In production, always use 'Custom' (the internal tile server).
        # OfflineHighres is only available as a fallback in dev mode.
        if DEV_MODE and not self.url:
            return "OfflineHighres"
        return "Custom"

    def as_dict(self) -> dict:
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
class ElasticsearchConfig:
    url: str
    ais_index: str
    user: str
    password: str

    @property
    def enabled(self) -> bool:
        return bool(self.url)


@dataclass(frozen=True)
class MongoConfig:
    url: str
    database: str

    @property
    def enabled(self) -> bool:
        return bool(self.url)


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)


@dataclass(frozen=True)
class PostgresConfig:
    dsn: str

    @property
    def enabled(self) -> bool:
        return bool(self.dsn)


@dataclass(frozen=True)
class RuntimeConfig:
    offline_mode: bool


FORWARDED_HOST = os.environ.get("FORWARDED_HOST", "http://localhost:8000")

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

SATVIS = SatvisConfig(
    default_tags=os.environ.get("SATVIS_DEFAULT_TAGS", "Resource"),
)

ELASTICSEARCH = ElasticsearchConfig(
    url=os.environ.get("ES_URL", "").rstrip("/"),
    ais_index=os.environ.get("ES_AIS_INDEX", "ais_positions"),
    user=os.environ.get("ES_USER", ""),
    password=os.environ.get("ES_PASS", ""),
)

MONGO = MongoConfig(
    url=os.environ.get("MONGO_URL", ""),
    database=os.environ.get("MONGO_DATABASE", "analytics"),
)

MINIO = MinioConfig(
    endpoint=os.environ.get("MINIO_ENDPOINT", ""),
    access_key=os.environ.get("MINIO_ACCESS_KEY", ""),
    secret_key=os.environ.get("MINIO_SECRET_KEY", ""),
    secure=_bool(os.environ.get("MINIO_SECURE", "1")),
)

POSTGRES = PostgresConfig(
    dsn=os.environ.get("POSTGRES_DSN", ""),
)

RUNTIME = RuntimeConfig(
    offline_mode=_bool(os.environ.get("DASHBOARD_OFFLINE_MODE", "")),
)
