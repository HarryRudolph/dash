from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from routes.data import router as data_router
from routes.pages import router as pages_router
from routes.satellite import router as satellite_router, satvis_router
from routes.vessel import router as vessel_router

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Vessel Dashboard")

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.include_router(pages_router, prefix="/dashboard")
app.include_router(data_router, prefix="/dashboard")
app.include_router(satellite_router, prefix="/dashboard")
app.include_router(satvis_router)
app.include_router(vessel_router, prefix="/dashboard")

# Satvis — built from the `next` branch of https://github.com/Flowm/satvis
satvis_dir = BASE_DIR / "static" / "satvis"
if satvis_dir.is_dir():
    app.mount("/satvis", StaticFiles(directory=satvis_dir, html=True), name="satvis")


@app.get("/")
async def root():
    return RedirectResponse(url="/dashboard")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
