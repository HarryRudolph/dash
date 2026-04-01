"""
Static page routes — Home, Vessels search.
"""

from fastapi import APIRouter, Request

from routes import templates

router = APIRouter()


@router.get("")
async def home(request: Request):
    return templates.TemplateResponse(request, "home.html")


@router.get("/vessels")
async def vessels(request: Request):
    return templates.TemplateResponse(request, "vessels.html")
