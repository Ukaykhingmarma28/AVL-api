#!/usr/bin/env python3
"""
HTTP + WebSocket API over avl_records, for a tracking frontend.

    set -a; . ./.env; set +a
    .venv/bin/python api_server.py            # http://127.0.0.1:8000/docs

REST
    GET /health
    GET /api/vehicles                          latest record per device
    GET /api/vehicles/{imei}                   latest record for one device
    GET /api/vehicles/{imei}/history           records in a time window

Realtime (both send a snapshot first, then one message per new record)
    WS  /api/stream/ws[?imei=...]
    GET /api/stream/sse[?imei=...]             Server-Sent Events

How "realtime" works
    The listener inserts rows; nothing here is in that path. New rows reach
    connected clients one of two ways, chosen at startup:

    listen  A trigger in schema.sql runs pg_notify() for every inserted row.
            One dedicated connection LISTENs and each notification is turned
            into a fetch of that row. Latency is the sink's flush interval
            plus a round trip. This is the intended mode.

    poll    Used when the trigger is not installed (a database created from
            an older schema.sql). A query every API_POLL_INTERVAL seconds
            picks up rows by received_at. Works, but adds up to one poll
            interval of latency and one query per interval, forever.

Environment
    DATABASE_URL        required
    PORT                default 8000 (Railway injects this)
    API_HOST            default 0.0.0.0
    API_KEY             if set, every request must present it as
                        X-API-Key, Authorization: Bearer, or ?api_key=
                        (the query form exists for WebSocket/EventSource,
                        which cannot set headers)
    API_CORS_ORIGINS    comma separated, default *
    API_REALTIME        auto | listen | poll, default auto
    API_POLL_INTERVAL   seconds, default 2
    API_LOG_LEVEL       default INFO
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Set

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket
from fastapi import WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

log = logging.getLogger("teltonika.api")

NOTIFY_CHANNEL = "avl_records"
NOTIFY_TRIGGER = "avl_records_notify"

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------


class Config:
    def __init__(self) -> None:
        env = os.environ
        self.dsn = env.get("DATABASE_URL", "")
        self.host = env.get("API_HOST", "0.0.0.0")
        self.port = int(env.get("PORT", "8000"))
        self.api_key = env.get("API_KEY") or None
        self.cors_origins = [
            o.strip() for o in env.get("API_CORS_ORIGINS", "*").split(",") if o.strip()
        ]
        self.realtime = env.get("API_REALTIME", "auto").lower()
        self.poll_interval = float(env.get("API_POLL_INTERVAL", "2"))
        self.log_level = env.get("API_LOG_LEVEL", "INFO").upper()

        if not self.dsn:
            raise SystemExit("DATABASE_URL not set. Run:  set -a; . ./.env; set +a")
        if self.realtime not in ("auto", "listen", "poll"):
            raise SystemExit("API_REALTIME must be auto, listen or poll")


# --------------------------------------------------------------------------
# Rows <-> JSON
# --------------------------------------------------------------------------

_RECORD_COLUMNS = """
    imei, ts, received_at, codec_id, priority, event_io_id,
    longitude, latitude, altitude_m, angle_deg, satellites, speed_kmh,
    fix_valid, io
"""

_LATEST_ALL = f"""
    SELECT DISTINCT ON (imei) {_RECORD_COLUMNS}
    FROM avl_records
    ORDER BY imei, ts DESC
"""

_LATEST_ONE = f"""
    SELECT {_RECORD_COLUMNS}
    FROM avl_records
    WHERE imei = $1
    ORDER BY ts DESC
    LIMIT 1
"""

_HISTORY = f"""
    SELECT {_RECORD_COLUMNS}
    FROM avl_records
    WHERE imei = $1 AND ts >= $2 AND ts < $3
      AND ($4::boolean IS FALSE OR fix_valid)
    ORDER BY ts
    LIMIT $5
"""

_BY_KEY = f"""
    SELECT {_RECORD_COLUMNS}
    FROM avl_records
    WHERE imei = $1 AND ts = $2 AND event_io_id = $3
"""

# The poll query looks back a little further than the cursor because two
# sink flushes can commit out of received_at order (the timer thread and a
# full-buffer flush race). Rows inside that lag window are deduplicated by
# primary key in Python.
_POLL = f"""
    SELECT {_RECORD_COLUMNS}
    FROM avl_records
    WHERE received_at > $1
    ORDER BY received_at
    LIMIT 5000
"""


def record_to_json(row: asyncpg.Record) -> Dict[str, Any]:
    d = dict(row)
    d["ts"] = d["ts"].isoformat()
    d["received_at"] = d["received_at"].isoformat()
    # Coordinates are only meaningful with a fix; the sink stores NULL then.
    return d


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


# --------------------------------------------------------------------------
# Fan-out to connected clients
# --------------------------------------------------------------------------


class Hub:
    """One queue per client. A client that stops reading loses the oldest
    messages rather than stalling everyone else."""

    QUEUE_SIZE = 1000

    def __init__(self) -> None:
        self._queues: Set[asyncio.Queue] = set()
        self.dropped = 0

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self.QUEUE_SIZE)
        self._queues.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._queues.discard(q)

    def publish(self, record: Dict[str, Any]) -> None:
        for q in self._queues:
            if q.full():
                q.get_nowait()
                self.dropped += 1
            q.put_nowait(record)

    @property
    def clients(self) -> int:
        return len(self._queues)


# --------------------------------------------------------------------------
# Feed: database -> hub
# --------------------------------------------------------------------------


class Feed:
    def __init__(self, cfg: Config, pool: asyncpg.Pool, hub: Hub) -> None:
        self.cfg = cfg
        self.pool = pool
        self.hub = hub
        self.mode = "starting"
        self.records_seen = 0
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        mode = self.cfg.realtime
        if mode == "auto":
            mode = "listen" if await self._trigger_installed() else "poll"
            if mode == "poll":
                log.warning(
                    "trigger %s is not installed; falling back to polling every "
                    "%.1fs. Re-run schema.sql (check_db.py --apply-schema) to "
                    "enable push notifications.",
                    NOTIFY_TRIGGER, self.cfg.poll_interval,
                )
        self.mode = mode
        runner = self._listen_forever if mode == "listen" else self._poll_forever
        self._task = asyncio.create_task(runner(), name="feed")
        log.info("realtime feed: %s", mode)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _trigger_installed(self) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_trigger "
                "WHERE tgrelid = 'avl_records'::regclass AND tgname = $1)",
                NOTIFY_TRIGGER,
            )

    # -- listen ------------------------------------------------------------

    async def _listen_forever(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._listen_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("listen connection failed; retrying in %.0fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _listen_once(self) -> None:
        # A dedicated connection: LISTEN is per-session, and a pooled
        # connection could be handed to a request in between notifications.
        conn = await asyncpg.connect(self.cfg.dsn, timeout=10)
        try:
            await _init_connection(conn)
            keys: asyncio.Queue = asyncio.Queue()

            def on_notify(_conn, _pid, _channel, payload: str) -> None:
                keys.put_nowait(payload)

            await conn.add_listener(NOTIFY_CHANNEL, on_notify)
            log.info("listening on channel %s", NOTIFY_CHANNEL)
            while True:
                # Wait for a key, with a periodic no-op query so a dead
                # connection is noticed instead of waiting forever.
                try:
                    payload = await asyncio.wait_for(keys.get(), timeout=30)
                except asyncio.TimeoutError:
                    await conn.execute("SELECT 1")
                    continue
                await self._deliver(conn, payload)
        finally:
            await conn.close()

    async def _deliver(self, conn: asyncpg.Connection, payload: str) -> None:
        try:
            key = json.loads(payload)
            ts = datetime.fromisoformat(key["ts"])
            row = await conn.fetchrow(_BY_KEY, key["imei"], ts, key["event_io_id"])
        except Exception:
            log.exception("bad notification payload: %r", payload)
            return
        if row is None:
            # The insert committed (NOTIFY is delivered on commit), so this
            # should not happen; log rather than guess.
            log.warning("notified row not found: %r", payload)
            return
        self.records_seen += 1
        self.hub.publish(record_to_json(row))

    # -- poll --------------------------------------------------------------

    async def _poll_forever(self) -> None:
        lag = timedelta(seconds=max(10.0, self.cfg.poll_interval * 3))
        cursor = datetime.now(timezone.utc)
        seen: Dict[tuple, datetime] = {}          # pk -> received_at
        while True:
            await asyncio.sleep(self.cfg.poll_interval)
            try:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(_POLL, cursor - lag)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("poll failed")
                continue
            for row in rows:
                pk = (row["imei"], row["ts"], row["event_io_id"])
                if pk in seen:
                    continue
                seen[pk] = row["received_at"]
                cursor = max(cursor, row["received_at"])
                self.records_seen += 1
                self.hub.publish(record_to_json(row))
            floor = cursor - lag
            seen = {k: v for k, v in seen.items() if v > floor}


# --------------------------------------------------------------------------
# App
# --------------------------------------------------------------------------

cfg = Config()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not cfg.api_key:
        log.warning("API_KEY is not set: anyone who can reach this port can "
                    "read every vehicle position")
    app.state.pool = await asyncpg.create_pool(
        cfg.dsn, min_size=1, max_size=5, init=_init_connection, timeout=10
    )
    app.state.hub = Hub()
    app.state.feed = Feed(cfg, app.state.pool, app.state.hub)
    await app.state.feed.start()
    try:
        yield
    finally:
        await app.state.feed.stop()
        await app.state.pool.close()


app = FastAPI(title="Teltonika fleet API", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg.cors_origins,
    allow_methods=["GET"],
    allow_headers=["X-API-Key", "Authorization"],
)


# -- auth ------------------------------------------------------------------


def _presented_key(headers, query) -> Optional[str]:
    if headers.get("x-api-key"):
        return headers["x-api-key"]
    auth = headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return query.get("api_key")


def _authorized(headers, query) -> bool:
    return cfg.api_key is None or _presented_key(headers, query) == cfg.api_key


async def require_key(request: Request) -> None:
    if not _authorized(request.headers, request.query_params):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing API key")


# -- REST ------------------------------------------------------------------


@app.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    try:
        async with request.app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db = "ok"
    except Exception as exc:
        db = f"error: {exc}"
    feed: Feed = request.app.state.feed
    return {
        "status": "ok" if db == "ok" else "degraded",
        "database": db,
        "realtime": feed.mode,
        "records_pushed": feed.records_seen,
        "clients": request.app.state.hub.clients,
        "messages_dropped": request.app.state.hub.dropped,
    }


@app.get("/api/vehicles", dependencies=[Depends(require_key)])
async def list_vehicles(request: Request) -> List[Dict[str, Any]]:
    """Latest record for every device that has ever reported."""
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch(_LATEST_ALL)
    return [record_to_json(r) for r in rows]


@app.get("/api/vehicles/{imei}", dependencies=[Depends(require_key)])
async def get_vehicle(request: Request, imei: str) -> Dict[str, Any]:
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(_LATEST_ONE, imei)
    if row is None:
        raise HTTPException(404, f"no records for imei {imei}")
    return record_to_json(row)


@app.get("/api/vehicles/{imei}/history", dependencies=[Depends(require_key)])
async def get_history(
    request: Request,
    imei: str,
    since: Optional[datetime] = Query(None, alias="from",
                                      description="ISO 8601, default 24h ago"),
    until: Optional[datetime] = Query(None, alias="to",
                                      description="ISO 8601, default now"),
    fix_only: bool = Query(True, description="drop records without a GPS fix"),
    limit: int = Query(1000, ge=1, le=10000),
) -> List[Dict[str, Any]]:
    """Records for one device, oldest first. Draw this as the track."""
    now = datetime.now(timezone.utc)
    until = _utc(until) or now
    since = _utc(since) or until - timedelta(hours=24)
    if since >= until:
        raise HTTPException(400, "'from' must be earlier than 'to'")
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch(_HISTORY, imei, since, until, fix_only, limit)
    return [record_to_json(r) for r in rows]


def _utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Query params without a zone are taken as UTC, like everything else here."""
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# -- realtime --------------------------------------------------------------


async def _snapshot(pool: asyncpg.Pool, imei: Optional[str]) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        if imei:
            row = await conn.fetchrow(_LATEST_ONE, imei)
            rows = [row] if row else []
        else:
            rows = await conn.fetch(_LATEST_ALL)
    return [record_to_json(r) for r in rows]


async def _events(app: FastAPI, imei: Optional[str]) -> AsyncIterator[Dict[str, Any]]:
    """Snapshot first, then every new record, filtered to one imei if asked.
    Yields a keepalive when nothing has happened for a while so proxies and
    clients can tell a quiet fleet from a dead connection."""
    hub: Hub = app.state.hub
    q = hub.subscribe()
    try:
        yield {"type": "snapshot", "vehicles": await _snapshot(app.state.pool, imei)}
        while True:
            try:
                record = await asyncio.wait_for(q.get(), timeout=15)
            except asyncio.TimeoutError:
                yield {"type": "keepalive"}
                continue
            if imei and record["imei"] != imei:
                continue
            yield {"type": "record", "record": record}
    finally:
        hub.unsubscribe(q)


@app.websocket("/api/stream/ws")
async def stream_ws(ws: WebSocket, imei: Optional[str] = None) -> None:
    if not _authorized(ws.headers, ws.query_params):
        await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    await ws.accept()

    async def drain_incoming() -> None:
        # Clients may send anything (ping, "hello"); we only care about close.
        while True:
            await ws.receive_text()

    async def send_events() -> None:
        async for event in _events(ws.app, imei):
            await ws.send_text(json.dumps(event))

    # Run both until either finishes: a close from the client ends the reader,
    # a failed send ends the writer. Whichever it is, the other is cancelled,
    # which also unsubscribes from the hub via the generator's finally.
    tasks = [asyncio.create_task(drain_incoming()), asyncio.create_task(send_events())]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            exc = t.exception()
            if exc and not isinstance(exc, (WebSocketDisconnect, RuntimeError)):
                raise exc
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@app.get("/api/stream/sse", dependencies=[Depends(require_key)])
async def stream_sse(request: Request, imei: Optional[str] = None) -> StreamingResponse:
    async def body() -> AsyncIterator[str]:
        async for event in _events(request.app, imei):
            if event["type"] == "keepalive":
                yield ": keepalive\n\n"
            else:
                yield f"event: {event['type']}\ndata: {json.dumps(event)}\n\n"

    return StreamingResponse(
        body(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --------------------------------------------------------------------------


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api_server:app",
        host=cfg.host,
        port=cfg.port,
        log_level=cfg.log_level.lower(),
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
