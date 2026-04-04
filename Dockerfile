FROM python:3.12-slim

WORKDIR /app

# Runtime configuration:
# - FORWARDED_HOST sets the external URL base (default http://localhost:8000).
# - DASHBOARD_DEV=1 enables mock satellite data endpoints.
# - DASHBOARD_OFFLINE_MODE=1 marks the deployment as air-gapped/offline.
# - SENSOR_NAME, SENSOR_LAT, SENSOR_LON configure the ground sensor metadata.
# - TILE_SERVER_URL enables slippy tile map ({z}/{y}/{x} format).
# - TILE_SERVER_CREDIT sets the imagery attribution string.
# - TILE_SERVER_MAX_ZOOM sets the imagery max zoom.
# - ES_URL, ES_USER, ES_PASS configure Elasticsearch with basic auth.
# - ES_AIS_INDEX sets the AIS positions index name.
# - MONGO_URL, MONGO_DATABASE configure MongoDB.
# - MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY configure MinIO.
# - POSTGRES_DSN configures PostgreSQL.
# - SZ_API_URL points at the Senzing-compatible REST API base URL.
# - SZ_DATA_SOURCE is the Senzing data source code for vessel record ids.
# - SZ_API_TOKEN optionally sets a bearer token for the SZ API.
# - SZ_TIMEOUT_SECONDS controls outbound SZ request timeout.
# - SZ_ENTITY_BY_RECORD_PATH and SZ_ENTITY_BY_ENTITY_PATH can override the
#   default REST path templates if your deployed SZ gateway uses different URLs.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 80

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "80", "--forwarded-allow-ips", "*", "--proxy-headers"]
