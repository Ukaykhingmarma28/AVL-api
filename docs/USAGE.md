# Using the AVL fleet API

A guide for building a frontend on top of this API: what each endpoint is
for, what the data means, and working code for the common screens of a
tracking app. For the endpoint reference and deployment, see the
[README](../README.md).

## Contents

1. [Setup: base URL and key](#1-setup-base-url-and-key)
2. [What a record looks like](#2-what-a-record-looks-like)
3. [Use case: live fleet map](#3-use-case-live-fleet-map)
4. [Use case: vehicle detail panel](#4-use-case-vehicle-detail-panel)
5. [Use case: online / offline status](#5-use-case-online--offline-status)
6. [Use case: trip history and replay](#6-use-case-trip-history-and-replay)
7. [Use case: alerts (ignition, speeding, low battery)](#7-use-case-alerts)
8. [Use case: fleet dashboard numbers](#8-use-case-fleet-dashboard-numbers)
9. [Staying connected: reconnects and keepalives](#9-staying-connected)
10. [Errors and status codes](#10-errors-and-status-codes)
11. [Consuming from Python or another backend](#11-consuming-from-python)
12. [Do and don't](#12-do-and-dont)

---

## 1. Setup: base URL and key

```js
const API  = "https://<your-railway-domain>";   // no trailing slash
const KEY  = "";                                // empty if API_KEY is not set on the server
```

If the server has `API_KEY` set, send it on every call:

| Client | How |
|---|---|
| `fetch` | header `X-API-Key: <key>` (or `Authorization: Bearer <key>`) |
| `WebSocket` / `EventSource` | query string `?api_key=<key>` (browsers cannot set headers here) |

Helpers used throughout this guide:

```js
const headers = KEY ? { "X-API-Key": KEY } : {};
const withKey = url => KEY ? `${url}${url.includes("?") ? "&" : "?"}api_key=${KEY}` : url;

async function get(path) {
  const r = await fetch(API + path, { headers });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}
```

Quick check that everything is wired up:

```
curl https://<domain>/health
→ {"status":"ok","database":"ok","realtime":"listen","records_pushed":812,"clients":2,"messages_dropped":0}
```

`realtime` should say `listen`. If it says `poll`, updates still arrive but
with up to 2 s extra delay; ask whoever runs the database to re-apply the
listener's `schema.sql`.

## 2. What a record looks like

Every endpoint returns the same object per record. One record is one AVL
report from one tracker.

```json
{
  "imei": "356307042441069",
  "ts": "2026-09-02T05:52:00.701000+00:00",
  "received_at": "2026-09-02T05:52:00.866388+00:00",
  "codec_id": 8,
  "priority": 0,
  "event_io_id": 0,
  "latitude": 23.8141877,
  "longitude": 90.4769379,
  "altitude_m": 19,
  "angle_deg": 52,
  "satellites": 12,
  "speed_kmh": 19,
  "fix_valid": true,
  "io": {
    "239": { "id": 239, "name": "Ignition",         "raw": 1,     "value": 1,     "state": "on" },
    "66":  { "id": 66,  "name": "External Voltage", "raw": 12840, "value": 12.84, "unit": "V" },
    "21":  { "id": 21,  "name": "GSM Signal",       "raw": 4,     "value": 4 }
  }
}
```

| Field | Meaning for the UI |
|---|---|
| `imei` | The vehicle's identity. Use it as the key for markers, rows, routes. |
| `ts` | When the tracker recorded the position (device clock, UTC). Show this as "last update". |
| `received_at` | When the server got it. `received_at - ts` is network lag; a large gap means the device was offline and is catching up. |
| `latitude`, `longitude` | `null` when `fix_valid` is false. Never plot a null. |
| `fix_valid` | False means no GPS lock (underground, just woke up). Keep the previous marker position and grey it out. |
| `speed_kmh`, `angle_deg` | Speed and heading. Rotate the marker icon by `angle_deg`. |
| `satellites` | Fix quality. Below 4 is poor. |
| `altitude_m` | Metres above sea level. |
| `event_io_id` | Which IO element triggered this report. `0` is a periodic report; `239` means ignition changed, etc. |
| `priority` | 0 low, 1 high, 2 panic. Panic is worth a visual alert. |
| `io` | Everything else the tracker reported, keyed by Teltonika IO id. |

### Reading `io`

Common ids you will actually use:

| id | name | notes |
|---|---|---|
| 239 | Ignition | `state` is `"on"` / `"off"` |
| 240 | Movement | `state` is `"moving"` / `"stopped"` |
| 66 | External Voltage | vehicle battery, `value` in volts |
| 67 | Battery Voltage | tracker's internal battery |
| 21 | GSM Signal | 0 to 5 |
| 200 | Sleep Mode | 0 awake |
| 16 | Total Odometer | metres |
| 24 | Speed | GNSS speed, usually equals `speed_kmh` |

A tiny helper so screens never index `io` by magic strings:

```js
const io = (rec, id) => rec.io?.[String(id)];
const ioValue = (rec, id, fallback = null) => io(rec, id)?.value ?? fallback;
const ioState = (rec, id, fallback = "unknown") => io(rec, id)?.state ?? fallback;

ioState(rec, 239)     // "on"
ioValue(rec, 66)      // 12.84
```

Ids missing from the server's `io_definitions.json` come back with
`name: null` and only `raw` / `value`. They are still real data.

---

## 3. Use case: live fleet map

Goal: a map with one marker per vehicle that moves as data arrives.

Pattern: open the stream. The first message is a **snapshot** with the
latest record for every vehicle; draw all markers from it. Every later
**record** message updates one marker. You never need to call
`/api/vehicles` separately for this screen.

```js
const markers = new Map();   // imei -> marker

function upsertMarker(rec) {
  if (!rec.fix_valid) {
    markers.get(rec.imei)?.setOpacity(0.4);   // keep last known spot, dim it
    return;
  }
  const pos = [rec.latitude, rec.longitude];
  let m = markers.get(rec.imei);
  if (!m) {
    m = L.marker(pos, { rotationAngle: rec.angle_deg }).addTo(map);  // Leaflet + rotatedMarker
    m.bindTooltip(rec.imei);
    markers.set(rec.imei, m);
  } else {
    m.setLatLng(pos);
    m.setRotationAngle(rec.angle_deg);
    m.setOpacity(1);
  }
}

const ws = new WebSocket(withKey(`${API.replace("https", "wss")}/api/stream/ws`));
ws.onmessage = ({ data }) => {
  const msg = JSON.parse(data);
  if (msg.type === "snapshot") msg.vehicles.forEach(upsertMarker);
  else if (msg.type === "record") upsertMarker(msg.record);
};
```

Server-Sent Events version, identical behaviour, and the browser reconnects
by itself:

```js
const es = new EventSource(withKey(`${API}/api/stream/sse`));
es.addEventListener("snapshot", e => JSON.parse(e.data).vehicles.forEach(upsertMarker));
es.addEventListener("record",   e => upsertMarker(JSON.parse(e.data).record));
```

Which one to pick: SSE if you only need server-to-client data (this API
never needs anything back), WebSocket if your stack already has a WS
client. Both carry the same messages.

React sketch with state instead of a marker map:

```jsx
function useFleet() {
  const [fleet, setFleet] = useState({});          // imei -> latest record
  useEffect(() => {
    const es = new EventSource(withKey(`${API}/api/stream/sse`));
    es.addEventListener("snapshot", e => {
      const v = JSON.parse(e.data).vehicles;
      setFleet(Object.fromEntries(v.map(r => [r.imei, r])));
    });
    es.addEventListener("record", e => {
      const r = JSON.parse(e.data).record;
      setFleet(f => ({ ...f, [r.imei]: r }));
    });
    return () => es.close();
  }, []);
  return fleet;
}
```

---

## 4. Use case: vehicle detail panel

Goal: click a vehicle, see its current state: position, speed, ignition,
battery, signal, last update time.

Two options.

**Already have the stream open** (from the map): you already hold the latest
record per vehicle in memory. Just render `fleet[imei]`.

**Standalone page** (deep link to one vehicle): fetch once, then subscribe to
that vehicle only so you are not woken by the rest of the fleet.

```js
let rec = await get(`/api/vehicles/${imei}`);
render(rec);

const es = new EventSource(withKey(`${API}/api/stream/sse?imei=${imei}`));
es.addEventListener("record", e => { rec = JSON.parse(e.data).record; render(rec); });

function render(r) {
  panel.innerHTML = `
    <h2>${r.imei}</h2>
    <div>Updated ${timeAgo(r.ts)}</div>
    <div>${r.fix_valid ? `${r.latitude.toFixed(5)}, ${r.longitude.toFixed(5)}` : "no GPS fix"}</div>
    <div>${r.speed_kmh} km/h, heading ${r.angle_deg}°</div>
    <div>Ignition: ${ioState(r, 239)}</div>
    <div>Battery: ${ioValue(r, 66, "?")} V</div>
    <div>Signal: ${ioValue(r, 21, "?")}/5, ${r.satellites} satellites</div>
  `;
}
```

The `?imei=` filter is applied server-side, so the browser receives only
that vehicle's records. The snapshot is also reduced to that one vehicle.

---

## 5. Use case: online / offline status

The API does not decide "online" for you, because the right threshold
depends on how the trackers are configured (a parked bus in deep sleep may
report every 10 minutes and still be fine). Derive it from the age of the
last record:

```js
function status(rec, now = Date.now()) {
  const age = (now - Date.parse(rec.ts)) / 1000;      // seconds
  const ignition = ioState(rec, 239) === "on";
  if (age > 15 * 60) return "offline";                // nothing for 15 min
  if (ignition && rec.speed_kmh > 0) return "moving";
  if (ignition) return "idling";
  return "parked";
}
```

Re-evaluate on a timer (every 30 s) as well as on each record, otherwise a
vehicle that stops reporting never flips to offline.

`received_at` vs `ts`: when a tracker comes back from a dead zone it uploads
its backlog. Those records have old `ts` and fresh `received_at`. The
stream delivers them in insert order, so your "latest" for that vehicle can
briefly go backwards in time. Guard against it:

```js
setFleet(f => {
  const cur = f[r.imei];
  return cur && cur.ts > r.ts ? f : { ...f, [r.imei]: r };   // ISO strings compare correctly
});
```

---

## 6. Use case: trip history and replay

Goal: draw where a vehicle went in a time window, or replay the day.

```js
const from = "2026-09-02T00:00:00Z";
const to   = "2026-09-03T00:00:00Z";
const track = await get(`/api/vehicles/${imei}/history?from=${from}&to=${to}&limit=10000`);

// polyline
const line = L.polyline(track.map(p => [p.latitude, p.longitude])).addTo(map);
map.fitBounds(line.getBounds());

// stop detection: consecutive points with ignition off or speed 0 for > 3 min
// speed profile: track.map(p => ({ t: p.ts, v: p.speed_kmh }))
```

Rules that matter:

- `from` and `to` are ISO 8601. Times without a zone are treated as UTC.
  Default window is the last 24 hours ending now.
- Records come back **oldest first**, ready for a polyline.
- `fix_only=true` (the default) skips records with no GPS fix, so the
  polyline has no `null` holes. Pass `fix_only=false` if you want to show
  "no fix" gaps or read IO values from those periods.
- `limit` caps at 10000. A tracker sending every second produces 86400
  points a day, so page through longer windows: fetch, take the last
  point's `ts` as the next `from`, repeat until fewer than `limit` rows come
  back.

```js
async function fullHistory(imei, from, to) {
  const out = [];
  let cursor = from;
  for (;;) {
    const page = await get(`/api/vehicles/${imei}/history?from=${cursor}&to=${to}&limit=10000`);
    out.push(...page);
    if (page.length < 10000) return out;
    cursor = page[page.length - 1].ts;   // ts >= from, so the last point repeats once; dedupe on (ts, event_io_id)
  }
}
```

Replay: iterate the array with a timer scaled by the gap between
consecutive `ts` values and move a single marker.

---

## 7. Use case: alerts

The stream gives you every record, so alerts are a comparison between the
previous record and the new one for the same vehicle. Do this in the
client, or in a small backend consumer (section 11) if alerts must fire when
nobody has the page open.

```js
const last = new Map();

function checkAlerts(rec) {
  const prev = last.get(rec.imei);
  last.set(rec.imei, rec);
  if (!prev) return;

  const ign = ioState(rec, 239), prevIgn = ioState(prev, 239);
  if (ign !== prevIgn) notify(rec.imei, `Ignition ${ign}`);

  if (rec.speed_kmh > 80 && prev.speed_kmh <= 80) notify(rec.imei, `Speeding: ${rec.speed_kmh} km/h`);

  const v = ioValue(rec, 66);
  if (v !== null && v < 11.5) notify(rec.imei, `Low battery: ${v} V`);

  if (rec.priority === 2) notify(rec.imei, "PANIC");
}
```

`event_io_id` is a shortcut: the tracker itself tells you *why* it sent a
record. `event_io_id === 239` means "this record exists because ignition
changed", so you can react without diffing.

---

## 8. Use case: fleet dashboard numbers

Counts like "12 moving, 30 parked, 3 offline" come from the same
`fleet` object you already keep for the map. Compute them on render:

```js
const counts = Object.values(fleet).reduce((c, r) => {
  const s = status(r);
  c[s] = (c[s] || 0) + 1;
  return c;
}, {});
```

Distance driven today per vehicle: fetch history with `fix_only=true` and
sum haversine distances between consecutive points, or read IO 16 (total
odometer, metres) from the first and last record of the day and subtract.
The odometer route is one subtraction and far more accurate than summing
GPS jitter.

Do not build dashboards by calling `/api/vehicles` on an interval. The
stream already delivers every change; polling adds load and latency for
nothing.

---

## 9. Staying connected

**Keepalives.** After 15 s with no records the server sends
`{"type":"keepalive"}` on WebSocket, or a `: keepalive` comment on SSE.
Ignore them in your message handler. Their job is to keep proxies from
closing an idle connection and to let you detect a dead one: if you have
seen nothing at all for 45 s, reconnect.

**Reconnecting.**

- `EventSource` reconnects automatically. On reconnect the server sends a
  fresh snapshot, so your state repairs itself. Nothing to do.
- `WebSocket` does not. Reconnect with backoff and rely on the snapshot to
  resync:

```js
function connect(delay = 1000) {
  const ws = new WebSocket(withKey(`${API.replace("https", "wss")}/api/stream/ws`));
  ws.onmessage = ({ data }) => handle(JSON.parse(data));
  ws.onclose = () => setTimeout(() => connect(Math.min(delay * 2, 30000)), delay);
  ws.onopen  = () => { delay = 1000; };
}
```

**What you miss while disconnected** is not replayed by the stream. The
snapshot gives you the current state; if you need the gap (for a track
being drawn live), fetch `/history?from=<last ts you saw>` for the vehicles
you care about after reconnecting.

**Slow clients.** The server buffers up to 1000 records per client. A tab
that is throttled in the background and cannot keep up loses the oldest
buffered records rather than stalling other clients. `messages_dropped` in
`/health` counts these across all clients. If it climbs, the frontend is
doing too much work per message; batch DOM updates with
`requestAnimationFrame`.

---

## 10. Errors and status codes

| Status | When | What to do |
|---|---|---|
| 200 | fine | |
| 400 | `from` is not before `to`, or a malformed timestamp | fix the query |
| 401 | `API_KEY` is set on the server and the request did not present it | send the key |
| 403 | WebSocket rejected for the same reason | add `?api_key=` |
| 404 | `/api/vehicles/{imei}` for an IMEI that has never reported | show "no data yet" |
| 422 | a query parameter of the wrong type (`limit=abc`) | fix the query |
| 5xx | database unreachable | retry with backoff; check `/health` |

Errors are JSON: `{"detail": "no records for imei 000"}`.

An empty array from `/api/vehicles` means no device has ever written to
the database. An empty array from `/history` means nothing in that window.
Neither is an error.

---

## 11. Consuming from Python

For a backend job (alerting, archiving, feeding another system) the SSE
stream is the simplest to consume, since it is plain HTTP:

```python
import json, httpx

API, KEY = "https://<domain>", ""

with httpx.stream("GET", f"{API}/api/stream/sse",
                  headers={"X-API-Key": KEY} if KEY else {}, timeout=None) as r:
    for line in r.iter_lines():
        if line.startswith("data:"):
            msg = json.loads(line[5:])
            if msg["type"] == "record":
                rec = msg["record"]
                print(rec["imei"], rec["ts"], rec.get("latitude"), rec.get("longitude"))
```

`timeout=None` matters: the connection is meant to stay open for hours.
Wrap it in a retry loop; on reconnect you get a fresh snapshot.

REST from Python is just `httpx.get(f"{API}/api/vehicles", headers=...)`.

---

## 12. Do and don't

**Do**

- Open one stream per page and drive everything (map, list, counts) from
  the in-memory `fleet` state it maintains.
- Use `?imei=` when a page is about one vehicle.
- Use `/history` for anything in the past; use the stream for anything
  happening now.
- Compare `ts` before overwriting a vehicle's latest record, so backlog
  uploads do not move markers backwards.
- Treat `fix_valid: false` as "position unknown right now", not as a point
  at 0,0.

**Don't**

- Don't poll `/api/vehicles` every few seconds. The stream exists so you
  never have to.
- Don't open a stream per vehicle on a fleet page. One unfiltered stream
  carries all of them.
- Don't put the API key in a public frontend if the data is sensitive. A
  key in browser JavaScript is visible to anyone who opens dev tools. For
  a public dashboard, leave `API_KEY` empty and accept that the data is
  public; for a private one, put the frontend behind its own login and
  proxy API calls through your backend, which holds the key.
- Don't rely on `keepalive` messages as data. They carry nothing.
