import asyncio
import logging
import os
import base64

from cassandra.cluster import Cluster, Session, SimpleStatement
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import DCAwareRoundRobinPolicy
from dotenv import load_dotenv
from fastapi import Request

load_dotenv()

logger = logging.getLogger(__name__)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "mathews_tracking")
CASSANDRA_REPLICATION_FACTOR = int(os.getenv("CASSANDRA_REPLICATION_FACTOR", "3"))

# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_KEYSPACE = f"""
CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {CASSANDRA_REPLICATION_FACTOR}}}
"""

_CREATE_GPS_BY_DRIVER = """
CREATE TABLE IF NOT EXISTS gps_by_driver (
    driver_id  text,
    day        date,
    ts         timestamp,
    lat        double,
    lng        double,
    heading    int,
    speed      double,
    order_id   int,
    PRIMARY KEY ((driver_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
"""

_CREATE_GPS_BY_ORDER = """
CREATE TABLE IF NOT EXISTS gps_by_order (
    order_id   int,
    ts         timestamp,
    driver_id  text,
    lat        double,
    lng        double,
    heading    int,
    speed      double,
    PRIMARY KEY (order_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
"""

# ── Startup ───────────────────────────────────────────────────────────────────


def create_cassandra_session() -> Session:
    """
    Abre la conexión con Cassandra, crea el keyspace y las tablas si no existen.
    Usa AsyncioConnection explícitamente — requerido en Python 3.12+ donde
    asyncore fue removido y libev no está disponible sin compilar extensiones C.
    Debe llamarse desde dentro del event loop (no via run_in_executor).
    """
    cluster = Cluster(
        [CASSANDRA_HOST],
        connection_class=AsyncioConnection,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        protocol_version=4,
    )
    session = cluster.connect()
    session.execute(_CREATE_KEYSPACE)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    session.execute(_CREATE_GPS_BY_DRIVER)
    session.execute(_CREATE_GPS_BY_ORDER)
    logger.info("Cassandra connected — keyspace '%s' ready", CASSANDRA_KEYSPACE)
    return session


# ── Async helper ──────────────────────────────────────────────────────────────


async def cassandra_execute(session: Session, statement: str, params=None):
    """
    Ejecuta un statement CQL de forma async envolviéndolo en un thread pool.
    cassandra-driver es sincrónico; run_in_executor evita bloquear el event loop.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, session.execute, statement, params or []
    )

def cassandra_execute_with_pagination(session: Session, statement, params=None, paging_state=None):
    results = session.execute(statement, params or [], paging_state=paging_state if paging_state else None)

    rows = results.current_rows

    next_paging_state = results.paging_state
    next_page_token = None
    
    if next_paging_state:
        next_page_token = base64.b64encode(next_paging_state).decode('utf-8')

    data = [row for row in rows]
    
    return {
        "data": data,
        "next_page_token": next_page_token
    }


# ── FastAPI dependency ────────────────────────────────────────────────────────


async def get_cassandra(request: Request) -> Session:
    return request.app.state.cassandra
