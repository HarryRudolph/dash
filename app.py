from pathlib import Path

import psycopg2
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from minio import Minio
from pymongo import MongoClient

from config import ELASTICSEARCH, FORWARDED_HOST, MINIO, MONGO, POSTGRES
from routes.data import router as data_router
from routes.pages import router as pages_router
from routes.satellite import router as satellite_router, satvis_router
from routes.vessel import router as vessel_router

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="dashboard",
    redirect_slashes=False,
)

app.mount("/dashboard/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.include_router(pages_router, prefix="/dashboard")
app.include_router(data_router, prefix="/dashboard")
app.include_router(satellite_router, prefix="/dashboard")
app.include_router(satvis_router)
app.include_router(vessel_router, prefix="/dashboard")

# Satvis — built from the `next` branch of https://github.com/Flowm/satvis
satvis_dir = BASE_DIR / "static" / "satvis"
if satvis_dir.is_dir():
    app.mount("/satvis", StaticFiles(directory=satvis_dir, html=True), name="satvis")


@app.on_event("startup")
async def startup():
    app.state.forwarded_host = f"{FORWARDED_HOST.rstrip('/')}/dashboard"

    # Elasticsearch
    if ELASTICSEARCH.enabled:
        es_kwargs = {
            "hosts": [ELASTICSEARCH.url],
            "verify_certs": False,
            "ssl_show_warn": False,
        }
        if ELASTICSEARCH.user and ELASTICSEARCH.password:
            es_kwargs["basic_auth"] = (ELASTICSEARCH.user, ELASTICSEARCH.password)
        app.state.es = AsyncElasticsearch(**es_kwargs)
    else:
        app.state.es = None

    # MongoDB
    if MONGO.enabled:
        app.state.mongo = MongoClient(MONGO.url)
        app.state.db = app.state.mongo[MONGO.database]
    else:
        app.state.mongo = None
        app.state.db = None

    # MinIO
    if MINIO.enabled:
        app.state.minio = Minio(
            MINIO.endpoint,
            access_key=MINIO.access_key,
            secret_key=MINIO.secret_key,
            secure=MINIO.secure,
        )
    else:
        app.state.minio = None

    # PostgreSQL
    if POSTGRES.enabled:
        app.state.pg = psycopg2.connect(POSTGRES.dsn)
    else:
        app.state.pg = None


@app.on_event("shutdown")
async def shutdown():
    if getattr(app.state, "es", None):
        await app.state.es.close()
    if getattr(app.state, "mongo", None):
        app.state.mongo.close()
    if getattr(app.state, "pg", None):
        app.state.pg.close()


@app.get("/")
async def root():
    return RedirectResponse(url="/dashboard")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
