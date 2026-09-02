# AVL fleet API

A small FastAPI service that reads the `avl_records` table written by the
[Teltonika listener](https://github.com/Ukaykhingmarma28/AVL) and hands it
to a browser: REST for "where is everything now" and history, plus a
WebSocket and a Server-Sent Events stream that push every new record the
moment it lands in the database. It never touches the device path; the
listener and its database schema live in that repo.

```
device ──▶ listener ──▶ TimescaleDB ──▶ api_server.py ──▶ frontend
                              │              ▲
                              └── pg_notify ─┘   (or a 2 s poll, see below)
```

## Run it locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env                  # fill in DATABASE_URL, API_KEY
set -a; . ./.env; set +a
.venv/bin/python api_server.py        # http://127.0.0.1:8000/docs
```

`/docs` is the interactive OpenAPI page; every endpoint below can be tried
from there.

## Endpoints

All endpoints except `/health` require the API key when `API_KEY` is set.

| Method | Path | Returns |
|---|---|---|
| GET | `/health` | database status, realtime mode, connected clients |
| GET | `/api/vehicles` | latest record per device, one array |
| GET | `/api/vehicles/{imei}` | latest record for one device, 404 if unknown |
| GET | `/api/vehicles/{imei}/history` | records oldest first; `from`, `to` (ISO 8601, default last 24 h), `fix_only` (default true), `limit` (default 1000, max 10000) |
| WS | `/api/stream/ws` | snapshot, then one message per new record; `?imei=` filters |
| GET | `/api/stream/sse` | the same over Server-Sent Events |

### Record shape

Every endpoint returns the same object per record, straight from the table:

```json
{
  "imei": "356307042441069",
  "ts": "2026-09-02T05:52:00.701000+00:00",
  "received_at": "2026-09-02T05:52:00.866388+00:00",
  "codec_id": 8, "priority": 0, "event_io_id": 0,
  "latitude": 23.8141877, "longitude": 90.4769379,
  "altitude_m": 19, "angle_deg": 52, "satellites": 12, "speed_kmh": 19,
  "fix_valid": true,
  "io": {
    "239": {"id": 239, "name": "Ignition", "raw": 1, "value": 1, "state": "on"},
    "66":  {"id": 66,  "name": "External Voltage", "raw": 12840, "value": 12.84, "unit": "V"}
  }
}
```

`ts` is the device clock, `received_at` the server clock. When `fix_valid`
is false the coordinates are `null`. `io` is keyed by Teltonika IO id; the
`name`, `unit`, `state` fields come from `io_definitions.json`, so an id that
is not catalogued there has `name: null` and only `raw`/`value`.

### Stream messages

The first message is always the current state, so a page can render the map
immediately and then apply updates:

```json
{"type": "snapshot", "vehicles": [ ...records... ]}
{"type": "record",   "record": { ...one record... }}
{"type": "keepalive"}
```

`keepalive` is sent after 15 s of silence on the WebSocket. On SSE the same
gap produces a `: keepalive` comment line, which `EventSource` ignores.

## Frontend snippets

WebSocket:

```js
const ws = new WebSocket(`wss://${host}/api/stream/ws?api_key=${key}`);
ws.onmessage = ({ data }) => {
  const msg = JSON.parse(data);
  if (msg.type === "snapshot") msg.vehicles.forEach(upsertMarker);
  if (msg.type === "record")   upsertMarker(msg.record);
};
```

Server-Sent Events (auto-reconnects for free):

```js
const es = new EventSource(`https://${host}/api/stream/sse?api_key=${key}`);
es.addEventListener("snapshot", e => JSON.parse(e.data).vehicles.forEach(upsertMarker));
es.addEventListener("record",   e => upsertMarker(JSON.parse(e.data).record));
```

Track for the last two hours:

```js
const r = await fetch(`https://${host}/api/vehicles/${imei}/history?from=${twoHoursAgo}`,
                      { headers: { "X-API-Key": key } });
const points = (await r.json()).map(p => [p.latitude, p.longitude]);
```

Browsers cannot set headers on `WebSocket` or `EventSource`, which is why the
key is also accepted as `?api_key=`. For `fetch` prefer the `X-API-Key`
header (or `Authorization: Bearer`) so the key stays out of access logs.

## How updates reach the client

At startup the server picks one of two modes and reports it in `/health`.

**listen** (intended). The listener repo's `schema.sql` installs a trigger that runs
`pg_notify('avl_records', {imei, ts, event_io_id})` for each inserted row.
The API holds one dedicated `LISTEN` connection, fetches each notified row,
and fans it out to every connected client. End-to-end latency is the sink's
flush interval (2 s) plus a round trip. Retransmitted batches hit
`ON CONFLICT DO NOTHING` and never fire the trigger, so clients do not see
duplicates.

**poll** (fallback). If the trigger is missing the server logs a warning
and instead queries for rows by `received_at` every `API_POLL_INTERVAL`
seconds. It works, but adds up to one interval of latency and costs a query
per interval forever.

To enable push on a database that was set up before the trigger existed,
re-apply the schema from the listener repo. It is idempotent:

```bash
# in the AVL (listener) checkout
.venv/bin/python check_db.py "$DATABASE_URL" --apply-schema
```

then restart the API (or set `API_REALTIME=listen` to force it).

## Security

Set `API_KEY`. Without it anyone who can reach the port can read every
vehicle position; the server logs a warning at startup to that effect.

`API_CORS_ORIGINS` defaults to `*`, which is acceptable only because the key
gates every request. Narrow it to the frontend's origin in production.

The service speaks plain HTTP. Terminate TLS in front of it (Railway does
this for you; on a droplet use Dokploy's Traefik or Caddy).

## Deploying

### Railway

Add a service from this repo in the same Railway project as the listener
and the database, so `DATABASE_URL` can use the private network.

1. **New → GitHub Repo**, pick this repo. `railway.json` selects the
   Dockerfile build and a `/health` check.
2. **Variables**:

   | Variable | Value |
   |---|---|
   | `DATABASE_URL` | `${{timescaledb.DATABASE_URL}}` (use your database service's name) |
   | `API_KEY` | `openssl rand -hex 24`, or empty for open access |
   | `API_CORS_ORIGINS` | your frontend's origin, or `*` |

3. **Settings → Networking → Generate Domain**. Railway injects `PORT` and
   the image listens on it; WebSockets and SSE work through Railway's HTTP
   edge. No volume is required: the API is stateless.

Check with `curl https://<domain>/health` and look for `"realtime":"listen"`.

### Docker anywhere else

```bash
docker build -t avl-api .
docker run -p 8000:8000 --env-file .env avl-api
```

The service speaks plain HTTP. Put a TLS reverse proxy (Caddy, Traefik,
nginx) in front of it; all of them proxy WebSockets and SSE without extra
configuration.
