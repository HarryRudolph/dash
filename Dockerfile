FROM python:3.12-slim

WORKDIR /app

# Runtime configuration:
# - DASHBOARD_DEV=1 enables mock satellite data endpoints.
# - DASHBOARD_OFFLINE_MODE=1 marks the deployment as air-gapped/offline.
# - SENSOR_NAME, SENSOR_LAT, SENSOR_LON configure the ground sensor metadata.
# - TILE_SERVER_URL enables Satvis Custom imagery.
# - TILE_SERVER_CREDIT sets the imagery attribution string.
# - TILE_SERVER_MAX_ZOOM sets the Satvis Custom imagery max zoom.
# - SZ_API_URL points at the Senzing-compatible REST API base URL.
# - SZ_DATA_SOURCE is the Senzing data source code for vessel record ids.
# - SZ_API_TOKEN optionally sets a bearer token for the SZ API.
# - SZ_TIMEOUT_SECONDS controls outbound SZ request timeout.
# - SZ_ENTITY_BY_RECORD_PATH and SZ_ENTITY_BY_ENTITY_PATH can override the
#   default REST path templates if your deployed SZ gateway uses different URLs.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
