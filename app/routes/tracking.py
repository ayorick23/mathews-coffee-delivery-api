import json
import time
import base64

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from cassandra.query import SimpleStatement
from app.cassandra import cassandra_execute, get_cassandra, cassandra_execute_with_pagination
from app.redis import get_redis

router = APIRouter()


# ── Schemas ───────────────────────────────────────────────────────────────────

class GPSPing(BaseModel):
    driver_id: str
    order_id: int
    lat: float
    lng: float
    heading: int
    speed: float


# ── GPS history queries ───────────────────────────────────────────────────────

@router.get("/drivers/{driver_id}/gps-history")
async def gps_history_by_driver(
    driver_id: str,
    date: str = Query(..., description="Fecha en formato YYYY-MM-DD"),
    page_token: str | None = Query(None, description="Token para paginación, obtenido de la respuesta anterior"),
    limit: int = Query(10, ge=1, le=100),
    cassandra=Depends(get_cassandra),
):
    """
    Retorna el historial GPS de un driver para un día específico.
    Consulta la tabla gps_by_driver particionada por (driver_id, day).
    """
    day = datetime.strptime(date, "%Y-%m-%d").date()

    query = "SELECT ts, lat, lng, heading, speed, order_id FROM gps_by_driver WHERE driver_id = %s AND day = %s"
    statement = SimpleStatement(query, fetch_size=limit)

    paging_state = None
    if page_token:
        try:
            paging_state = base64.b64decode(page_token)
        except Exception:
            raise HTTPException(status_code=400, detail="Token de paginación inválido")

    result = cassandra_execute_with_pagination(cassandra, statement, (driver_id, day), paging_state=paging_state)

    points = [
        {
            "ts": row.ts.isoformat(),
            "lat": row.lat,
            "lng": row.lng,
            "heading": row.heading,
            "speed": row.speed,
            "order_id": row.order_id,
        }
        for row in result["data"]
    ]
    return {"driver_id": driver_id, "date": date, "points": points, "next_page_token": result["next_page_token"]}


@router.get("/orders/{order_id}/gps-trail")
async def gps_trail_by_order(
    order_id: int,
    page_token: str | None = Query(None, description="Token para paginación, obtenido de la respuesta anterior"),
    limit: int = Query(10, ge=1, le=100),
    cassandra=Depends(get_cassandra),
):
    """
    Retorna el trail GPS completo de una orden.
    Consulta la tabla gps_by_order particionada por order_id.
    """

    query = "SELECT ts, lat, lng, heading, speed, driver_id FROM gps_by_order WHERE order_id = %s"
    statement = SimpleStatement(query, fetch_size=limit)

    paging_state = None
    if page_token:
        try:
            paging_state = base64.b64decode(page_token)
        except Exception:
            raise HTTPException(status_code=400, detail="Token de paginación inválido")

    result = cassandra_execute_with_pagination(cassandra, statement, (order_id,), paging_state=paging_state)

    points = [
        {
            "ts": row.ts.isoformat(),
            "lat": row.lat,
            "lng": row.lng,
            "heading": row.heading,
            "speed": row.speed,
            "driver_id": row.driver_id,
        }
        for row in result["data"]
    ]
    return {"order_id": order_id, "points": points, "next_page_token": result["next_page_token"]}
