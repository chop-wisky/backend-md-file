# Backend Implementation Spec — Real-Time News Platform

Companion to the architecture diagram. This is the level of detail Cursor needs to scaffold the actual backend. All open decisions from the original draft have been resolved below (marked **Resolved**) with reasoning, except two intentionally deferred items (load-testing tool choice, real-account auth fields) that depend on decisions outside this spec's scope.

## 0. Repo & folder structure

Monorepo, one Railway service per top-level folder under `services/`. Shared types/schemas live in one package so `ingest-service`, `broadcast-worker`, and `persistence-worker` all import the same shape instead of redefining it.

**Note — Centrifugo lives in its own separate repository, not in this monorepo.** It's deployed as its own Railway service, but into the **same Railway project** as everything below (required for private internal networking between services — see section 1 for what breaks if it's in a different project). Its config (`centrifugo-config.json`, JWT validation setup) lives in that repo, not under `services/gateway/` here. This monorepo only holds the code that *talks to* Centrifugo (`broadcast-worker`'s publish client, `gateway`'s nginx routing to it) — never Centrifugo's own source.

**Note — nginx also lives in its own separate repository, same pattern as Centrifugo.** It deploys as its own Railway service, in the same Railway project as everything else (so it can reach the internal URLs of `ingest-service`, `calendar-webhook`, and Centrifugo). Its `nginx.conf` lives in that repo, not under `services/gateway/` in this monorepo — keeping it separate means routing/proxy changes deploy independently from application code changes, and the config isn't tied to this repo's release cycle.

```
/
├── services/
│   ├── ingest-service/
│   │   ├── src/
│   │   │   ├── routes/
│   │   │   ├── middleware/       # auth check, rate limit
│   │   │   ├── validators/
│   │   │   └── index.ts
│   │   ├── package.json
│   │   └── railway.json
│   │
│   ├── calendar-webhook/
│   │   ├── src/
│   │   │   ├── routes/
│   │   │   ├── validators/       # HMAC check, dedup, field validation
│   │   │   └── index.ts
│   │   └── package.json
│   │
│   ├── broadcast-worker/
│   │   ├── src/
│   │   │   ├── consumers/        # Redis Stream consumer (group:broadcast-workers)
│   │   │   ├── centrifugo/       # publish client — calls CENTRIFUGO_API_URL, lives here even though Centrifugo itself doesn't
│   │   │   └── index.ts
│   │   └── package.json
│   │
│   ├── persistence-worker/
│   │   ├── src/
│   │   │   ├── consumers/        # Redis Stream consumer (group:persistence-workers)
│   │   │   ├── queue/            # BullMQ setup, retry/backoff config
│   │   │   ├── db/                # Postgres writes, one file per table
│   │   │   └── index.ts
│   │   └── package.json
│   │
│   └── cot-fetch-worker/
│       ├── src/
│       │   ├── fetch/             # primary + fallback source logic
│       │   ├── staleness.ts
│       │   └── index.ts
│       └── package.json
│
├── packages/
│   └── shared-types/
│       ├── src/
│       │   ├── calendarEvent.ts   # matches section 2.2 payload shape
│       │   ├── marketData.ts      # matches section 2.3
│       │   ├── streamMessage.ts   # matches section 2.4 envelope
│       │   └── index.ts
│       └── package.json
│
├── migrations/                    # SQL files, matches section 5 schema
│   ├── 001_calendar_events.sql
│   ├── 002_cot_data.sql
│   ├── 003_failed_writes.sql
│   └── 004_users.sql
│
├── .env.example                   # matches section 7
└── package.json                   # workspace root (npm/pnpm workspaces)
```

**Separate repos (not part of this tree):**
```
centrifugo-service/                # own repo, own deploy, own Railway service
├── centrifugo-config.json         # channel namespaces, JWT claim mapping (section 2.1)
├── Dockerfile                     # or Railway's Centrifugo template if using one
└── railway.json

gateway-nginx/                     # own repo, own deploy, own Railway service
├── nginx.conf                     # routes to ingest-service, calendar-webhook, and Centrifugo — all via internal Railway URLs
├── Dockerfile                     # FROM nginx:alpine, COPY nginx.conf in
└── railway.json
```

Each service's `package.json` depends on `shared-types` via workspace reference (e.g. `"shared-types": "workspace:*"`), so a field change in one payload shape only needs updating in one place.

## 1. Services to scaffold

| Service | Responsibility | Runtime |
|---|---|---|
| `gateway` | Nginx edge — **separate repo**, HTTP + WS routing, TLS termination | Docker (nginx:alpine) |
| `ingest-service` | Lightweight shim, auth check, rate limit, hands off to workers | Node.js |
| `broadcast-worker` | Fast lane — writes to Redis Streams, pushes to Centrifugo | Node.js |
| `persistence-worker` | Safe lane — consumes Redis Streams, writes Postgres via BullMQ | Node.js |
| `cot-fetch-worker` | Scheduled Friday-night CFTC pull + fallback mirror | Node.js (cron) |
| `calendar-webhook` | Receives MQL5 EA POSTs | Node.js (Express route) |
| `centrifugo` | Real-time pub/sub broker — **separate repo**, deployed as its own Railway service inside the same Railway project | Centrifugo binary, 3× Railway instances |
| `redis` | Streams + cache-aside for `/api/market/:symbol` with stampede lock (section 2.3) — 3 instances: 1 primary + 2 replicas for HA (see section 10.1) | Managed Redis (Railway plugin or Upstash) |
| `postgres` | Durable store | Managed Postgres (Railway plugin) |

**What nginx in `gateway` is actually doing** (so Cursor scaffolds `nginx.conf` with the right routing, not a generic reverse proxy):

1. **Single public entry point.** One domain/cert in front of `ingest-service`, `calendar-webhook`, and Centrifugo, instead of each getting its own separate public URL. Path-based routing: `/auth/*` and `/api/*` → `ingest-service`, `/webhook/calendar-event` → `calendar-webhook`, `/connection/websocket` (or your Centrifugo WS path) → Centrifugo.
2. **TLS termination.** nginx holds the cert; internal services talk plain HTTP behind it.
3. **WebSocket proxying to Centrifugo.** Requires `proxy_http_version 1.1` plus `Upgrade`/`Connection` headers forwarded, and long idle timeouts (`proxy_read_timeout`) since these are long-lived connections, not request/response — the default nginx proxy timeout will kill idle WS connections if not raised.
4. **Load balancing across the 3 Centrifugo instances.** `upstream` block with round-robin (or `least_conn`) across the 3 nodes so incoming WS connections distribute evenly at connect-time.
5. **Request-level guards before traffic reaches app code.** Basic rate limiting (`limit_req`) and request size caps (`client_max_body_size`) at the nginx layer, as a first line of defense in front of the rate limits already specified per-endpoint in section 8 — not a replacement for them, both layers apply.

If `gateway`/nginx is dropped in favor of Railway's own ingress + folding auth into `ingest-service`, items 3 and 4 above (WS upgrade handling, load balancing across Centrifugo nodes) need to be re-implemented in application code or handled by Centrifugo's own client-side node discovery instead — they don't happen automatically without something doing this job.

**Traffic spike handling — concrete `nginx.conf` values (not just defaults):**

nginx is event-driven and handles high concurrency well natively, but only if configured for it — the out-of-the-box defaults are tuned for a small server, not 100k concurrent WebSocket clients. Three things need explicit tuning:

1. **Raise connection ceiling** (default `worker_connections` is often 512–1024, far too low):
```nginx
worker_processes auto;              # one per available CPU core
events {
    worker_connections 4096;        # per worker — tune up alongside container CPU/RAM
    multi_accept on;
}
```
Also raise the container's OS-level file descriptor limit (`ulimit -n`) to match — nginx will refuse new connections past this even if `worker_connections` allows more, since every connection is a file descriptor.

2. **Shed/throttle excess before it reaches app code**, don't just relay everything through:
```nginx
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=20r/s;
limit_req_zone $binary_remote_addr zone=webhook_limit:10m rate=5r/s;

location /api/ {
    limit_req zone=api_limit burst=40 nodelay;
    proxy_pass http://ingest-service.railway.internal:PORT;
}

location /webhook/calendar-event {
    limit_req zone=webhook_limit burst=10 nodelay;
    proxy_pass http://calendar-webhook.railway.internal:PORT;
}

client_max_body_size 1m;            # reject oversized payloads before they reach app code
```
These numbers should match section 8's per-endpoint rate limits — nginx enforcing them too is a first line of defense, not a replacement; `ingest-service` still enforces its own per-user limits (nginx can't see the JWT `sub` claim, only IP).

3. **WebSocket-specific timeouts** (default proxy timeouts will kill idle-but-connected WS clients — a WS connection with no traffic for a minute isn't dead, it's just quiet):
```nginx
location /connection/websocket {
    proxy_pass http://centrifugo_upstream;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 3600s;        # long-lived connections, not request/response
    proxy_send_timeout 3600s;
}
```

**What nginx surviving a spike does *not* guarantee:** nginx being event-driven means it won't fall over from connection volume alone, but it happily forwards everything it's not explicitly told to throttle — a spike that passes through untouched can still overwhelm `ingest-service` (Node, doing real DB work) even while nginx itself is fine. The `limit_req` zones above are what actually protects the backend, not nginx's raw concurrency handling.

**Reconnect-storm scenario** (all clients briefly disconnect then reconnect at once — e.g. after a deploy): nginx's load balancing across the 3 Centrifugo instances (item 4 above) spreads this load, but the real fix is client-side — the Flutter app's reconnect logic needs jitter (random delay before retrying, not all clients retrying at the same instant) to prevent the herd from forming in the first place. This belongs in the Flutter app's WebSocket client code, not in the backend spec, but worth flagging so it isn't missed.

## 2. API contracts

### 2.1 Auth

```
POST /auth/token
Body:    { "userId": "string", "deviceId": "string" }
Response 200:
{
  "token": "eyJhbGciOi...",      // JWT, signed HS256
  "expiresIn": 3600,              // seconds
  "channels": ["market:euro_fx"]  // channels this user is pre-authorized for
}
```

JWT claims:
```json
{
  "sub": "userId",
  "device": "deviceId",
  "channels": ["market:euro_fx"],
  "iat": 1234567890,
  "exp": 1234571490
}
```
- Expiry: 1 hour (`3600`s), refresh via same endpoint. **Resolved** — matches `JWT_EXPIRY_SECONDS` default in section 7.
- **Resolved — JWT signing: HS256 shared secret** (faster to ship than JWKS; revisit only if you add third-party token issuers). `POST /auth/token` is handled by `ingest-service` (nginx has no application logic — it only proxies to this route), which signs with `JWT_SECRET`; Centrifugo validates with `CENTRIFUGO_JWT_SECRET`. **These must be the same value** — set both env vars to one secret, or better, have `centrifugo-config.json` read `JWT_SECRET` directly and drop `CENTRIFUGO_JWT_SECRET` from `.env.example` entirely to remove the chance of drift.
- **Channel authorization enforcement**: `ingest-service` is the only place `channels` is checked. On `/auth/token`, it looks up which channels the user is entitled to and bakes that list into the JWT claims. Centrifugo itself does not re-derive entitlement — it trusts the `channels` claim in the token it already validated. If a client's subscribe request names a channel not in that claim, Centrifugo rejects the subscription (standard Centrifugo behavior for JWT-scoped channels). Do not add a second authorization check anywhere else; one source of truth.

### 2.2 MQL5 EA Webhook (Origin A — live push)

**The EA's entire job is this one POST request.** It has no knowledge of Centrifugo, WebSockets, or the frontend — it POSTs to this endpoint and its responsibility ends there. Everything downstream (deciding broadcast priority, pushing live to clients, persisting to Postgres) is handled entirely by the backend services below, not the EA.

```
POST /webhook/calendar-event
Headers: X-EA-Signature: <HMAC-SHA256 of body, hex>
Body:
{
  "eventId": "string",          // unique per calendar event instance
  "symbol": "EUR",              // currency/symbol code
  "eventName": "string",        // e.g. "Non-Farm Payrolls"
  "impact": "high" | "medium" | "low",
  "actual": "number|null",
  "forecast": "number|null",
  "previous": "number|null",
  "releaseTime": "ISO8601 string, UTC — MUST end in 'Z' (e.g. 2026-07-18T12:30:00Z), never a local-time offset",
  "seqId": "integer"            // EA-assigned sequence number, for EA-side dedup/logging only — NOT the canonical seqId (see note below)
}

Response 200: { "received": true, "seqId": 10234 }
Response 400: { "error": "validation_failed", "field": "..." }
Response 401: { "error": "invalid_signature" }
Response 429: { "error": "rate_limited" }
```

**Validation rules** (this is where the hash-set sentinel bug lived — be explicit in code):
- `eventId` + `releaseTime` combo must be unique — reject duplicates idempotently (return 200 with `"duplicate": true`, not an error, so the EA's retry queue doesn't loop)
- `impact` must be one of the 3 enum values, reject otherwise
- Numeric fields (`actual`, `forecast`, `previous`) — accept `null` explicitly, reject non-numeric strings
- HMAC signature check happens **before** JSON parsing of business fields
- **`releaseTime` must be UTC, reject anything else** — validate it parses to a `Z`-suffixed (or `+00:00`) ISO8601 string; reject with `400 validation_failed` if the EA sends a local-time offset instead. This matters specifically because the MetaTrader terminal running the EA could be configured to any broker/server timezone — if the EA ever sent its local time unconverted, every downstream consumer (Postgres, Centrifugo, the Flutter app) would silently show the wrong time with no way to detect it after the fact.

**Resolved — timezone handling end-to-end.** The whole pipeline follows one rule: **store and transmit UTC everywhere, convert to local time only at final display.**
- **EA (MQL5) side**: must convert to UTC before sending, regardless of what timezone the broker's MetaTrader server clock uses (broker server time is often *not* UTC — commonly UTC+2/+3 for many forex brokers). This conversion has to happen in the EA's own code before building the JSON body; the backend has no way to correct a wrong timezone after the fact, so this is worth testing explicitly, not assumed.
- **Backend (all services)**: never converts or interprets `releaseTime` — stores and forwards it exactly as UTC, in Postgres (`TIMESTAMPTZ` column type, not naive `TIMESTAMP`, so timezone-correctness is enforced at the schema level too — see section 5), and in the Centrifugo message envelope (section 2.4) unchanged.
- **Flutter app**: converts UTC → device local time at render time only, using the OS's own timezone (`DateTime.parse(releaseTimeUtc).toLocal()` in Dart) — no user-facing timezone setting needed, this is the same automatic behavior established economic calendar sites (Forex Factory, Investing.com) already rely on.

**Resolved — `seqId` naming clash**: the EA sends its own `seqId` in the request body (useful for EA-side logging/dedup only). The webhook does **not** persist or forward that value as the canonical sequence number. Once validation passes, the webhook calls `XADD stream:market-events *` (section 3), and the Redis-assigned stream ID is mapped to the integer that becomes the *canonical* `seqId` — this is what's returned in the `200` response, what's written to `calendar_events.seq_id`, and what appears in the Centrifugo message envelope (section 2.4). Rename the inbound field to `eaSeqId` in `shared-types/calendarEvent.ts` so the two values can't be confused in code — `eaSeqId` (client-supplied, informational) vs `seqId` (server-assigned, canonical).

**EA-side (MQL5) implementation notes** — this isn't Cursor's job since it's not part of this backend repo, but hand this to whoever writes the EA:

*What HMAC is doing here, briefly:* both sides share a secret (`EA_WEBHOOK_HMAC_SECRET`, section 7). The EA hashes the exact JSON body it's about to send, combined with that secret, using HMAC-SHA256 — producing a signature. It sends that signature in the `X-EA-Signature` header alongside the body. The webhook recomputes the same hash on its end and rejects the request (`401`) if the signatures don't match. This stops anyone who doesn't know the secret from posting fake events to this public URL — the backend has no other way to tell a real EA request from a forged one.

*MQL5-specific concerns to flag early, before the EA is written:*
- **`WebRequest()` requires the destination URL to be manually whitelisted** in MetaTrader (Tools → Options → Expert Advisors → "Allow WebRequest for listed URL") on whichever machine/VPS runs the EA — this is a one-time manual step per install, not something the backend can automate or bypass.
- **HMAC-SHA256 is not a built-in MQL5 function.** Confirm early whether the specific MetaTrader build being used has a crypto/hash library available (some brokers' MQL5 environments include `CryptEncode()` with `CRYPT_HASH_SHA256`, but HMAC specifically — hash-with-a-key, not just hash — may need to be implemented manually by XORing the key with padding per the standard HMAC construction, or by calling out to a small helper). Worth a spike/prototype before assuming this is a quick add.
- **The exact byte sequence being hashed must match on both sides.** JSON key ordering, whitespace, and number formatting (e.g. `41234` vs `41234.0`) all change the hash output — if MQL5's JSON serialization doesn't produce byte-identical output to what the webhook expects, signatures will never match even with the correct secret. Safest approach: have the EA build the exact JSON string itself (not rely on a library's serializer that might format differently), and have the webhook hash the **raw request body bytes it received**, not a re-serialized version — this is why validation rule 4 above says HMAC check happens before JSON parsing, not after.

### 2.3 Market data (client-facing read)

```
GET /api/market/:symbol
Response 200:
{
  "symbol": "EUR",
  "cot": {
    "asOf": "2026-07-14",
    "stale": false,
    "source": "primary",
    "contractUnit": "EUR 125,000",
    "openInterest": 799495,
    "openInterestChange": 4662,
    "totalTraders": 327,
    "nonCommercial": {
      "long": 230307, "short": 242912, "spreads": 28745,
      "longChg": 6877, "shortChg": 3255, "spreadsChg": 1544,
      "longPct": 28.8, "shortPct": 30.4, "spreadsPct": 3.6,
      "longTraders": 83, "shortTraders": 60, "spreadsTraders": 31
    },
    "commercial": {
      "long": 457594, "short": 470984,
      "longChg": -3075, "shortChg": 291,
      "longPct": 57.2, "shortPct": 58.9,
      "longTraders": 133, "shortTraders": 103
    },
    "nonReportable": {
      "long": 82849, "short": 56854,
      "longChg": -684, "shortChg": -428,
      "longPct": 10.4, "shortPct": 7.1
    },
    "netPosition": -12605,
    "netPositionChg": 3622
  },
  "sentiment": { "retailLongPct": 72 },
  "calendarEvents": [ /* recent events, same shape as webhook body */ ]
}
```

- **Resolved — full category breakdown, verified against live CFTC Legacy data** (EUR FX, code 099741, as of 2026-07-14 — see conversation for source screenshots). Two corrections from the first draft of this response shape, both now reflected above and in `cot_data`'s schema (section 5):
  1. **Non-Commercial has a `spreads` field** (long/short/spreads, not just long/short) — this category alone reports simultaneous long+short positions separately; Commercial and Non-Reportable don't have this field.
  2. **Trader counts are per Long/Short/Spreads column, not one number per category** — e.g. Non-Commercial had 83 long-side traders, 60 short-side, 31 spread-side in the live data, not a single combined "46 traders" figure. `nonReportable` has no trader-count fields at all (confirmed absent in live data, not just null) — Flutter should not render a trader-count row for that category rather than showing a dash for a field that doesn't conceptually exist there.
  3. **`totalTraders`** is CFTC's own single summary figure (327 in the live example) — do not compute this by summing the per-category trader counts, since a single trader can appear across multiple category rows and summing would double-count.
- `netPosition` here is `noncommercial_long - noncommercial_short` (the generated column, section 5) — worth double-checking this is still the definition you want for "net position" now that Commercial has its own long/short too; some COT-based apps define "net position" against Commercial instead of Non-Commercial, or show both. Flag if you want `netPosition` to reflect Commercial positioning instead of/in addition to Non-Commercial — that would need a second generated column, not a redefinition of this one, since some users may want to compare both.
- `commercial`/`nonCommercial` naming here uses CFTC's Legacy-report terminology directly (not TFF's Dealer/Asset Manager/Leveraged Fund/Other split) — **this only works if `cot-fetch-worker` pulls from CFTC's Legacy table**, not TFF. Worth flagging: section 6 currently has `cot-fetch-worker` pulling from TFF for currencies (correct for forex-specific granularity) and Disaggregated for commodities — **neither of those tables has a "Commercial/Non-Commercial" split**, that terminology is specific to the **Legacy** report. This needs a decision before Cursor builds the fetch logic — see the note at the end of section 6.
- `stale: true` + `source: "fallback"` fields are what power the "Last Updated" badge — **required in the schema now**, not optional

**Caching — resolved: cache-aside with stampede protection.** Without this, every request hits Postgres directly, which won't hold up under the rate limits in section 8 (20 req/s per user × many users). This endpoint's data also changes in bursts (news spikes) rather than steadily, which is exactly the pattern that causes cache stampedes if not handled deliberately.

```
Cache key:   market:cache:<symbol>
TTL:         10s during normal periods
             2s while a high-impact calendar event for that symbol is within ±5 min (tighter freshness when it matters most)
```

**Read path (`ingest-service` handling `GET /api/market/:symbol`):**
1. `GET market:cache:<symbol>` from Redis. Hit → return immediately.
2. Miss → **acquire a per-symbol lock** before querying Postgres: `SET market:lock:<symbol> 1 NX PX 3000` (3s expiry as a safety net in case the holder crashes).
   - **Lock acquired** → query Postgres, write result to `market:cache:<symbol>` with the TTL above, `DEL` the lock, return the result.
   - **Lock not acquired** (another request is already refilling this key) → poll the cache key every ~50ms for up to 500ms waiting for the lock-holder to populate it, then return that. If it still hasn't populated after 500ms, fall through and query Postgres directly for this one request only (don't hold the herd hostage to one slow query) — do not re-acquire the lock, just read-through and return without writing cache (the original lock-holder will still write it).
3. This means at most **one** Postgres query per symbol per cache-refill event, regardless of how many thousands of clients request that symbol in the same instant — the rest either get the cached result or briefly wait on the lock.

**Why lock-based over alternatives:** a simpler "just add jitter to TTL" approach (randomizing expiry ±2s per key) reduces synchronized expiry across *different* symbols but doesn't stop a stampede on the *same* symbol during a real spike, which is the actual failure mode here (one symbol, one major news event, mass simultaneous requests). The lock directly bounds concurrent Postgres load to 1 query per symbol, which jitter alone doesn't guarantee.

**Cache invalidation on write:** `persistence-worker` should `DEL market:cache:<symbol>` immediately after successfully writing a new calendar event, COT update, or sentiment update for that symbol (section 4) — don't wait for TTL expiry when we already know the data changed; this also naturally shortens the effective staleness window during exactly the high-traffic periods where it matters.

**Primary vs. replica reads — target the primary for this whole flow.** Redis replication (section 10.1) is asynchronous: a write to `market:cache:<symbol>` lands on the primary first, then copies to the 2 replicas milliseconds later. If the lock-acquisition check, the cache-hit read, or the 500ms polling loop above were pointed at a replica, a read could land in that brief replication gap and see a false miss even though the primary was just written — causing extra Postgres queries to slip through and partially defeating the stampede protection. Route all of this endpoint's Redis reads (steps 1–2 above) to the **primary**, not a replica. Replicas are still useful for read-scaling elsewhere (e.g. `GET /api/replay`'s `XRANGE` calls, section 10.5, where a few milliseconds of staleness is harmless) — just not for this stampede-sensitive path where correctness of the lock/cache-check depends on reading the most current state.

**New — `GET /api/market/:symbol/history` (needed for Flutter charting, not just numbers).** A single `netPosition` value is close to meaningless to a trader without the trend behind it — is positioning building or reversing? That's only visible as a chart, and a chart needs multiple data points, which the current single-snapshot endpoint above can't provide. Good news: no schema change needed — `cot_data`'s `UNIQUE(symbol, as_of)` constraint (section 5) already means every weekly fetch gets its own row rather than overwriting the last one, so the history already exists in Postgres; it's just not exposed yet.

```
GET /api/market/:symbol/history?weeks=12
Response 200:
{
  "symbol": "GBP",
  "history": [
    { "asOf": "2026-07-14", "netPosition": 41234, "netPositionChg": -1812, "openInterest": 285647 },
    { "asOf": "2026-07-07", "netPosition": 43046, "netPositionChg": 2210,  "openInterest": 283643 }
    /* ...ordered most-recent-first, up to `weeks` entries */
  ]
}
```

- **Kept intentionally thin** — history is for charting trend lines (net position over time), not for re-rendering the full Commercial/Non-Commercial breakdown table at every historical week. If the UI ever wants to show the full category breakdown for a past week (not just the current one), extend this response rather than bloating every row by default — most weeks in a 12-52 week chart are never individually inspected in full detail.
- `weeks` query param: default 12, max 104 (2 years) — cap it so a request can't pull the entire multi-year history in one call and blow up response size unnecessarily
- Query: `SELECT as_of, net_position, net_position_chg, open_interest FROM cot_data WHERE symbol = $1 ORDER BY as_of DESC LIMIT $2` — simple, indexed on the existing `UNIQUE(symbol, as_of)` constraint, no new index needed. Note this now reads the **generated** `net_position` column (section 5) rather than a stored value — Postgres computes it automatically from `noncommercial_long`/`noncommercial_short` at write time, nothing extra needed in this query.
- **Cache this too**, same pattern as section 2.3's main endpoint but with a much longer TTL — COT data only updates once a week (Friday), so a 1-hour cache TTL on this specific endpoint is safe and cuts repeated Postgres load from every user opening a chart. No stampede-lock complexity needed here — the low update frequency means a stampede on this specific endpoint isn't a realistic risk the way the main market endpoint's news-spike scenario is.
- This is a `cot_data`-only endpoint for now (matches what's actually multi-week in the schema); `calendarEvents` and `sentiment` don't currently have the same historical row-per-fetch pattern — if trend charts are wanted for those too later, that's a separate schema check, not assumed to already work the same way.

### 2.4 WebSocket / Centrifugo

```
Channel naming convention:
  market:<symbol>            e.g. market:euro_fx
  calendar:<impact>          e.g. calendar:high     (optional secondary channel for alert-only subscribers)

Message envelope (published to Redis Stream, then broadcast):
{
  "seqId": 10234,
  "channel": "market:euro_fx",
  "type": "cot_update" | "calendar_event" | "sentiment_update",
  "payload": { ... },
  "publishedAt": "ISO8601"
}
```

- **Resolved — `calendar:<impact>` is a real secondary channel**, not just a client-side filter. Reasoning: filtering client-side means every client still receives every event over the wire regardless of interest, which defeats the point of an alert-only tier and wastes bandwidth/battery on mobile. `broadcast-worker` publishes high-impact calendar events to both `market:<symbol>` and `calendar:high` — same message, two channels, one `XADD`-derived `seqId`. Low/medium-impact events publish to `market:<symbol>` only.

## 3. Redis Streams — naming & consumer groups

```
Stream: stream:market-events
  - Single stream for all event types, filtered by `channel` field in the message
  - **Resolved — start with a single stream.** Calendar events are inherently low-volume (scheduled economic releases, at most a handful per minute even during busy sessions); COT updates are once-weekly. Splitting streams adds consumer-group bookkeeping with no throughput benefit at this volume. Revisit only if a specific event type needs independent backpressure/scaling from the others — there's no such requirement today.

Consumer groups:
  - group:broadcast-workers   → reads stream, pushes to Centrifugo (fast lane)
  - group:persistence-workers → reads same stream, writes Postgres via BullMQ (safe lane)

Both groups read independently — one group's ack doesn't block the other.
This is what gives you fast broadcast + safe persistence without one blocking the other.
```

**seq_id assignment**: use Redis Stream's own auto-generated ID (`XADD stream:market-events * ...`) as the canonical `seqId` — don't generate it client-side, avoids collisions. The 15-char hyphenated ID Redis gives you (`timestamp-sequence`) can be mapped to a simpler incrementing integer if the client needs a plain number for gap detection.

## 4. BullMQ (write-ahead queue)

```
Queue: postgres-write-queue

Job data shape:
{
  "table": "cot_data" | "calendar_events" | "sentiment",
  "operation": "insert" | "upsert",
  "data": { ...row fields... },
  "seqId": 10234
}

Retry config:
  attempts: 5
  backoff: { type: "exponential", delay: 2000 }   // 2s, 4s, 8s, 16s, 32s

On final failure (after 5 attempts):
  → move to dead-letter table `failed_writes`
  → fire alert via Slack incoming webhook (**Resolved** — see `ALERT_WEBHOOK_URL` in section 7; POST a simple JSON payload with `original_table`, truncated `error_message`, and a count of unresolved `failed_writes` rows)
```

`failed_writes` table columns: `id, original_table, payload (jsonb), error_message, failed_at, retry_count`

## 5. Postgres schema (minimum viable)

```sql
CREATE TABLE calendar_events (
  id            BIGSERIAL PRIMARY KEY,
  event_id      TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  event_name    TEXT NOT NULL,
  impact        TEXT NOT NULL CHECK (impact IN ('high','medium','low')),
  actual        NUMERIC,
  forecast      NUMERIC,
  previous      NUMERIC,
  release_time  TIMESTAMPTZ NOT NULL,
  seq_id        BIGINT NOT NULL,
  created_at    TIMESTAMPTZ DEFAULT now(),
  UNIQUE(event_id, release_time)
);

-- Expanded (was net_position/change_pct only) to support the full Legacy-style category
-- breakdown a real COT UI needs: Non-Commercial / Commercial / Non-Reportable, each Long/Short,
-- plus open interest, % of OI per category, and trader counts. This matches CFTC's own report
-- structure directly (Legacy report format), rather than a single derived "net position" number.
-- Verified against live CFTC Legacy data (EUR FX, code 099741, as of 2026-07-14) — see below for
-- the corrections that came from checking real output instead of assuming the field list.
CREATE TABLE cot_data (
  id                    BIGSERIAL PRIMARY KEY,
  symbol                TEXT NOT NULL,
  as_of                 DATE NOT NULL,

  -- Open interest (total contracts outstanding, and week-over-week change)
  open_interest         BIGINT NOT NULL,
  open_interest_change  BIGINT,                        -- vs previous week; null on first-ever fetch for a symbol

  -- Non-Commercial (speculators) — CONFIRMED from live data: has a "Spreads" column the earlier
  -- draft of this schema was missing entirely. Spreads = simultaneous long+short positions held
  -- by the same trader (e.g. calendar spreads) — reported separately because they don't represent
  -- net directional bets the way plain long/short do. Needed for schema accuracy even if the UI
  -- doesn't display it initially.
  noncommercial_long        BIGINT NOT NULL,
  noncommercial_short       BIGINT NOT NULL,
  noncommercial_spreads     BIGINT NOT NULL DEFAULT 0,
  noncommercial_long_chg    BIGINT,
  noncommercial_short_chg   BIGINT,
  noncommercial_spreads_chg BIGINT,

  -- Commercial (hedgers) — confirmed: no Spreads column for this category in live data
  commercial_long       BIGINT NOT NULL,
  commercial_short      BIGINT NOT NULL,
  commercial_long_chg   BIGINT,
  commercial_short_chg  BIGINT,

  -- Non-Reportable (small traders below CFTC's reporting threshold)
  nonreportable_long       BIGINT NOT NULL,
  nonreportable_short      BIGINT NOT NULL,
  nonreportable_long_chg   BIGINT,
  nonreportable_short_chg  BIGINT,

  -- % of open interest per category (CFTC publishes these directly, don't recompute client-side —
  -- avoids float rounding mismatches between backend-calculated and CFTC-reported percentages).
  -- CONFIRMED live data also includes a %-of-OI figure for Spreads specifically — added below.
  noncommercial_long_pct     NUMERIC(5,2),
  noncommercial_short_pct    NUMERIC(5,2),
  noncommercial_spreads_pct  NUMERIC(5,2),
  commercial_long_pct        NUMERIC(5,2),
  commercial_short_pct       NUMERIC(5,2),
  nonreportable_long_pct     NUMERIC(5,2),
  nonreportable_short_pct    NUMERIC(5,2),

  -- Trader counts per category — CORRECTED: live data shows these are per Long/Short/Spreads
  -- column, not one combined number per category as the earlier draft assumed (e.g. Non-Commercial
  -- showed 83 long-side traders, 60 short-side, 31 spread-side — three distinct counts, not one).
  -- Non-Reportable has no trader count in CFTC's own data (shown as blank/dash in the UI, stays null).
  noncommercial_long_traders     INTEGER,
  noncommercial_short_traders    INTEGER,
  noncommercial_spreads_traders  INTEGER,
  commercial_long_traders        INTEGER,
  commercial_short_traders       INTEGER,
  total_traders                  INTEGER,  -- CFTC reports this as one summary figure ("Total Traders: 327" in the live data), not derived by summing the above (categories overlap — a trader can appear in multiple rows)

  -- Derived convenience fields — computed once at write time in persistence-worker, not per-request,
  -- so `/api/market/:symbol`'s simple summary view (section 2.3) and history endpoint don't need to
  -- recompute net position from the raw longs/shorts on every read
  net_position          BIGINT GENERATED ALWAYS AS (noncommercial_long - noncommercial_short) STORED,
  net_position_chg       BIGINT,                        -- vs previous week's net_position; computed at write time, not generated, since it needs the prior row

  contract_unit          TEXT,                           -- e.g. "GBP 62,500" — display string from CFTC's contract size, shown in the UI header
  source                TEXT NOT NULL DEFAULT 'primary',  -- 'primary' | 'fallback'
  is_stale               BOOLEAN NOT NULL DEFAULT false,
  fetched_at             TIMESTAMPTZ DEFAULT now(),
  UNIQUE(symbol, as_of)
);

CREATE TABLE failed_writes (
  id              BIGSERIAL PRIMARY KEY,
  original_table  TEXT NOT NULL,
  payload         JSONB NOT NULL,
  error_message   TEXT,
  retry_count     INT DEFAULT 0,
  failed_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE users (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  device_id   TEXT,
  created_at  TIMESTAMPTZ DEFAULT now()
  -- Intentionally left open: add email/OAuth fields only if/when real accounts replace anonymous device auth (see section 8 note)
);
```

## 6. Error handling / staleness rules

```
COT fetch (Friday night):
  1. Attempt primary (CFTC) fetch
  2. On failure → retry once immediately
  3. On second failure → attempt fallback mirror (Tradingster or similar)
  4. On fallback success → write with source: 'fallback', is_stale: false
  5. On fallback failure too → keep last known good row, mark is_stale: true
  6. Client badge logic: if is_stale → show "STALE DATA — FALLBACK (Xhrs ago)"
                          else show "Last Updated: X mins ago — Primary Source"
```

**New — how COT data actually reaches Redis, end to end.** This wasn't spelled out explicitly before — COT data touches Redis in **two separate, unrelated ways**, each with its own purpose and its own expiry rules. Conflating them is an easy mistake, so laying out both clearly:

**1. The event stream (`stream:market-events`) — for pushing the live update to connected clients**
- After `cot-fetch-worker` successfully writes the new `cot_data` row to Postgres (per the error-handling flow above), it calls `XADD stream:market-events *` with a message envelope (section 2.4 shape, `type: "cot_update"`) containing the new snapshot.
- `broadcast-worker` picks this up off the stream (same fast-lane path every other event type uses, section 3) and pushes it to Centrifugo, which fans it out to any client currently subscribed to `market:<symbol>`.
- **No expiry here in the traditional sense** — this is a stream entry, not a cache key. It lives in the stream until trimmed by the `MAXLEN` policy (section 10.4), same as every other event type. A client that's online at the moment of the Friday-night fetch sees the update pushed live; a client that's offline simply gets the new value the next time it calls `GET /api/market/:symbol` (which reads from Postgres/cache, not the stream).

**2. The read cache (`market:cache:<symbol>`) — for serving `GET /api/market/:symbol` fast**
- This is the cache-aside pattern from section 2.3 — same key, same stampede-lock mechanism already specced there. COT data doesn't get a separate cache key; it's part of the same combined `market:cache:<symbol>` blob that also holds calendar events and sentiment for that symbol.
- **Expiry for this key is already specced in section 2.3: 10s normally, 2s near a high-impact event.** That TTL was written with calendar-event freshness in mind (news moves fast), not COT (which only changes once a week) — worth being explicit that the *same* short TTL applies here too, since COT is bundled into the same cached object as the fast-moving calendar data. This is intentional, not an oversight: splitting COT into its own longer-TTL cache key would mean two separate cache lookups (and two possible stampede scenarios) per request instead of one, for a single combined endpoint that already returns both at once. The tradeoff is a slightly wasteful re-fetch of unchanged COT data every 10s during active browsing — acceptable, since **section 2.3's invalidation-on-write already handles the actual efficiency concern**: `persistence-worker` calls `DEL market:cache:<symbol>` the moment a new COT row lands (once a week), not on any fixed timer, so the "every 10s" TTL rarely triggers a real Postgres re-fetch for COT specifically — most 10s windows just re-serve identical cached COT data alongside whatever calendar data changed.
- **The separate `GET /api/market/:symbol/history` endpoint (section 2.3) has its own independent cache key and 1-hour TTL** — that one *is* COT-specific and *does* get the longer expiry, since it's not bundled with fast-moving calendar data the way the main endpoint is.

**In short: COT data is pushed to Redis the same way every other event type is (via the stream, for live clients) and cached the same way every other market-data field is (via the shared per-symbol cache key, for fast reads) — there's no COT-specific Redis path to build. The one COT-specific expiry rule is the 1-hour TTL on the separate `/history` endpoint's cache.**

**Resolved — CFTC "no API key" is not actually a blocker.** CFTC publishes COT data through a **Socrata Open Data (SODA) API** at `publicreporting.cftc.gov` — this is a real REST/JSON API, not just a downloadable report, and it requires **no sign-up, no auth header, no API key** for the request volumes this app needs (one fetch per week). What the spec was missing was the actual endpoint and query shape — added below.

**Correction — use TFF, not Disaggregated, for forex symbols.** An earlier draft of this spec pointed at the Disaggregated table as the default. That's wrong for currency pairs specifically: Disaggregated is built for physical-commodity markets (agriculture, energy, metals); currencies are financial futures, and **Traders in Financial Futures (TFF)** is the report CFTC — and the trading community generally — treats as authoritative for forex positioning (it separates Dealers/Asset Managers/Leveraged Funds/Other, which is the breakdown retail forex traders actually care about; Disaggregated's Producer/Swap Dealer/Managed Money categories are commodity-oriented and don't map cleanly onto currency-pair sentiment). Disaggregated is still correct if you later add physical-commodity symbols (gold, oil, etc.) — just not for the currency symbols this app currently tracks.

```
Primary endpoint (TFF Combined — futures + options, correct table for currency/financial-futures symbols):
GET https://publicreporting.cftc.gov/resource/yw9f-hn96.json?$where=<filter>&$order=report_date_as_yyyy_mm_dd DESC&$limit=1

Query params (SODA/SoQL syntax):
  $where   — e.g. contract_market_name='EURO FX' to filter to one instrument
  $order   — report_date_as_yyyy_mm_dd DESC to get the most recent report first
  $limit   — 1 for latest only, or omit + add $offset for historical backfill
  $select  — optionally restrict to just the fields cot-fetch-worker actually needs (net position, report date, contract name) to reduce payload size

Response: JSON array of report rows. No auth header required for this request volume — Socrata's public rate limit (unauthenticated) is generous enough for once-weekly polling; only needed a token/key if doing high-frequency or bulk historical pulls, which this app doesn't.
```

**Other CFTC report tables available at the same pattern** (different resource IDs, same query syntax):
- **TFF Combined** (`yw9f-hn96.json`) — used above, correct default for currency-pair symbols **and** stock index futures (CFTC's own TFF coverage explicitly includes "stocks" and equity indexes alongside currencies — same table, different `contract_market_name` values)
- **Disaggregated Combined** (`kh3c-gbw2.json`) — required for metals and other physical-commodity symbols (gold, silver, copper, oil, etc.) — **do not use TFF for these**, the category breakdown doesn't apply
- **Legacy** — broadest historical coverage (back to 1986), simplest category breakdown; not needed unless historical backfill predates TFF/Disaggregated's 2006 start date

**Partially resolved — symbol → `contract_market_name` mapping table.** Still needs your final symbol list to be 100% complete, but major currencies, common metals, and the most common index futures are mapped below and ready to hardcode. **Important: metals and indexes don't all use the same CFTC table as currencies** — this is now a three-way split, not one lookup:

**Currencies — TFF table** (`yw9f-hn96.json`):

| Your `symbol` | `contract_market_name` value | CFTC contract code |
|---|---|---|
| `EUR` | `EURO FX` | 099741 |
| `GBP` | `BRITISH POUND STERLING` | 096742 |
| `JPY` | `JAPANESE YEN` | 097741 |
| `AUD` | `AUSTRALIAN DOLLAR` | 232741 |
| `NZD` | `NEW ZEALAND DOLLAR` | 112741 |
| `MXN` | `MEXICAN PESO` | 095741 |

**USD Index — different table entirely, not TFF.** This was missing because it genuinely isn't part of the same lookup as the other currencies above — **the U.S. Dollar Index isn't a CFTC-native contract**, it's traded on **ICE Futures U.S.**, and CFTC reports on it under the **Legacy** report (not TFF, not Disaggregated). Confirmed live: `contract_market_name` is `USD INDEX - ICE FUTURES U.S.`, code **098662**, currently reported under Legacy's Non-Commercial/Commercial/Non-Reportable + Spreads structure (the same category layout confirmed earlier for EUR FX) — so this one actually fits your UI's category labels natively, without needing the Legacy-vs-TFF table-choice decision (section 6) resolved first, unlike the other currencies.

| Your `symbol` | `contract_market_name` value | CFTC contract code | Table |
|---|---|---|---|
| `USD` (Dollar Index) | `USD INDEX - ICE FUTURES U.S.` | 098662 | **Legacy**, not TFF |

If option 1 from section 6 is chosen (Legacy for everything), USD fits the same lookup and fetch logic as every other symbol with zero special-casing. If option 2 is chosen (keep TFF/Disaggregated for other currencies/metals), `cot-fetch-worker` needs an explicit third routing branch just for USD, since it's the only symbol that would use Legacy while everything else uses TFF/Disaggregated — worth factoring into that decision, since it tips slightly further in favor of option 1's simplicity.

**Metals & other physical commodities — Disaggregated table** (`kh3c-gbw2.json`), *different resource ID than currencies/indexes*:

| Your `symbol` | Likely `contract_market_name` value | CFTC contract code |
|---|---|---|
| `XAU` (Gold) | `GOLD` | 088691 |
| `XAG` (Silver) | `SILVER` | 084691 |
| `COPPER` | `COPPER-#1` (exact formatting needs live verification — hyphenation/spacing may differ in the live API field) | 085692 |
| `WTI` / `OIL` (Crude Oil) | `CRUDE OIL, LIGHT SWEET-WTI` (confirmed as NYMEX physical delivery contract, "CL" — the standard benchmark contract; note ICE also lists a separate WTI contract under a different code, don't confuse the two) | 067651 |
| `NATGAS` (Natural Gas) | `NAT GAS NYME` (confirmed NYMEX Henry Hub contract; ICE also lists a separate Natural Gas contract under a different code — 023391 — don't confuse the two) | 023651 |
| `WHEAT` | `WHEAT-SRW` (Soft Red Winter, the CBOT benchmark contract — confirm this is the class you want; Kansas City Hard Red Winter and Minneapolis Hard Red Spring wheat trade as separate contracts if a different class is intended) | 001602 |
| `CORN` | `CORN` | 002602 |
| `SOYBEAN` | `SOYBEANS` (exact plural/singular form needs live verification) | 005602 |

Platinum and Palladium (NYMEX) also exist in this table if you want to track them later — not looked up yet.

**Indexes — TFF table** (`yw9f-hn96.json`), *same table as currencies, just a different `contract_market_name`*:

| Your `symbol` | Likely `contract_market_name` value | CFTC contract code |
|---|---|---|
| `SPX` (S&P 500 E-mini) | `E-MINI S&P 500` | 13874A |

Nasdaq 100 (E-mini, code 20974+) and Dow Jones (E-mini, code 12460+) are also in TFF's index coverage but I haven't confirmed their exact `contract_market_name` strings — add once you confirm which indexes are in scope. **Note from CFTC's own release history:** the E-mini contracts were consolidated with their parent contracts under codes like `13874+` for reporting purposes at some point — worth confirming in a live API pull whether the code/name you get back is `13874A` or the consolidated `13874+` form, since this affects the exact `$where` filter string.

**Critical — resolved: table choice conflict between forex-precision (TFF) and the UI's Commercial/Non-Commercial labeling (Legacy).** The UI mockup (section 2.3) shows Commercial/Non-Commercial/Non-Reportable categories — that's **Legacy report terminology specifically**. TFF (used above for currencies/indexes) uses a different 4-way split entirely (Dealer/Asset Manager/Leveraged Fund/Other), and Disaggregated (used for metals/commodities) uses yet another split (Producer/Swap Dealer/Managed Money/Other Reportable). None of these three tables share the same category names — **you cannot mix TFF data into a Commercial/Non-Commercial-labeled UI without either relabeling the UI or switching data sources.**

Two ways to resolve this, pick one:
1. **Switch `cot-fetch-worker` to pull from the Legacy table for all symbols** (currencies, indexes, and commodities all have Legacy coverage — it's CFTC's oldest, simplest, most broadly-covered report). Loses TFF's finer-grained Dealer/Asset Manager/Leveraged Fund breakdown for forex specifically, but matches the UI exactly with zero relabeling, and simplifies `cot-fetch-worker` to one table for everything instead of three.
2. **Keep TFF/Disaggregated for data richness, relabel the UI per table.** TFF's categories would show as Dealer/Asset Mgr/Leveraged Fund/Other instead of Commercial/Non-Commercial/Non-Reportable; Disaggregated's would show as Producer/Swap Dealer/Managed Money/Other Reportable. More accurate to what each table actually measures, but means three different category-label sets across your symbol types, and the `cot_data` schema (section 5) would need category names as configurable labels per row, not hardcoded `commercial_*`/`noncommercial_*` column names.

**Recommendation: option 1 (Legacy for everything), unless the extra granularity in TFF/Disaggregated is something you specifically want to show later.** Legacy is simpler to build, matches the UI as designed with no relabeling work, and the resource ID it needs (`6dca-aqww.json` for Legacy Combined — **not yet verified live, flag for confirmation**) follows the same SODA pattern as the other two tables. This changes `CFTC_TFF_SOURCE_URL`/`CFTC_DISAGGREGATED_SOURCE_URL` (section 7) to a single `CFTC_LEGACY_SOURCE_URL`, and every `contract_market_name` mapping above needs re-verification against the Legacy table specifically, since Legacy's exact naming may differ slightly from TFF/Disaggregated's for the same instrument.

**This decision blocks `cot-fetch-worker` implementation — needs your confirmation (option 1 or 2) before Cursor builds the fetch logic**, since the table choice determines the entire schema/response shape already committed to in sections 2.3 and 5.

**Still open — needs your confirmation before Cursor hardcodes any of this:**
- **Every `contract_market_name` string above should be verified against a live API response** before shipping — for each table, run `GET .../<resource-id>.json?$limit=5` and inspect the actual field values returned, rather than trusting the string as typed here. CFTC's naming/formatting (hyphens, spacing, plural/singular, "E-MINI" vs "EMINI") has enough inconsistency across contracts that an exact-match `$where` filter needs to be checked against real data, not assumed.
- **CAD (Canadian Dollar) and CHF (Swiss Franc)** — commonly tracked forex pairs, not yet looked up; add once your symbol list confirms scope.
- **Which indexes beyond S&P 500** — Nasdaq, Dow, others? — needs your list.
- **Wheat class** — CBOT SRW is used above as the default/benchmark; confirm this is actually the class you want, since Kansas City and Minneapolis wheat are separate contracts with separate CFTC codes.
- **Oil and Natural Gas contract choice** — NYMEX contracts are used above (the standard US benchmark contracts); ICE lists separate contracts for both under different codes — confirm NYMEX is the intended source, not ICE.
- This table should live in `cot-fetch-worker/src/fetch/symbolMapping.ts` (or a JSON config in the same folder) as **one lookup object per table** (`tffSymbols`, `disaggregatedSymbols`) rather than a single flat map, since `cot-fetch-worker`'s fetch logic now needs to know which endpoint to call per symbol, not just which filter string to use. A plain lookup, not scattered `if` statements — adding a new instrument later becomes a one-line addition per table, not a code change to the fetch logic itself.
- `cot-fetch-worker`'s `fetch/` module should route each of your app's tracked `symbol`s (section 2.3) to the correct table (TFF for currencies/indexes, Disaggregated for metals/commodities) and the right `contract_market_name` value within it — the tables above now cover currencies, indexes, metals, energy, and grains; finalize once your full symbol list is set.

## 7. Environment variables (for Cursor to scaffold `.env.example`)

```
# Redis
REDIS_URL=
REDIS_STREAM_KEY=stream:market-events

# Postgres
DATABASE_URL=

# Centrifugo
CENTRIFUGO_API_URL=
CENTRIFUGO_API_KEY=
CENTRIFUGO_JWT_SECRET=

# Auth
JWT_SECRET=
JWT_EXPIRY_SECONDS=3600

# MQL5 Webhook
EA_WEBHOOK_HMAC_SECRET=

# CFTC fetch — no API key needed (public SODA endpoint, see section 6). Two URLs now: TFF for currencies/indexes, Disaggregated for metals — see symbolMapping.ts for which symbol uses which. Kept as env vars so resource IDs can change without a code deploy.
# CFTC fetch — no API key needed (public SODA endpoint, see section 6). PENDING: table choice conflict unresolved (Legacy vs TFF/Disaggregated) — see section 6 "table choice conflict" note. These two URLs are correct IF option 2 is chosen; if option 1 (Legacy for everything, recommended), replace both with a single CFTC_LEGACY_SOURCE_URL pointing at the Legacy Combined resource ID instead.
CFTC_TFF_SOURCE_URL=https://publicreporting.cftc.gov/resource/yw9f-hn96.json
CFTC_DISAGGREGATED_SOURCE_URL=https://publicreporting.cftc.gov/resource/kh3c-gbw2.json
CFTC_FALLBACK_URL=

# Alerting
ALERT_WEBHOOK_URL=   # Slack incoming webhook, or similar
```

## 8. Decisions (resolved)

- [x] Single Redis stream vs. split per event-type → **single stream** (section 3)
- [x] JWT signing: shared secret (HS256) vs. JWKS → **HS256 shared secret**, one secret reused by gateway and Centrifugo (section 2.1)
- [x] Alert channel for dead-letter failures → **Slack incoming webhook** (section 4)
- [x] Whether `calendar:<impact>` is a real secondary channel or just a client-side filter → **real secondary channel** (section 2.4)
- [x] Rate limit numbers — **Resolved**, see below
- [x] Redis topology → **3 instances: 1 primary + 2 read replicas** for HA (section 10.1)

**Resolved — rate limits:**
- `/webhook/calendar-event`: 5 req/s per source IP (single EA instance posting scheduled events — this is generous headroom, not a real ceiling). Return `429` per the existing contract.
- `/api/market/:symbol`: 20 req/s per authenticated `userId` (from JWT `sub`), 100 req/s per IP as a secondary ceiling to catch pre-auth abuse.
- `/auth/token`: 10 req/min per `deviceId` — enough for normal refresh cycles, tight enough to blunt token-minting abuse.
These are starting values for launch, not load-tested — revisit after the k6/Artillery pass in section 9.

**Gap closed — `users` table / auth flow**: the spec as written accepts any `userId` + `deviceId` pair at `/auth/token` with no signup or verification step, which is fine *if* this is intentional anonymous/device-based auth (no email, no password). Confirm that's the actual intent before Cursor scaffolds it — if real user accounts (email, OAuth, etc.) are needed later, `users` will need those columns added and `/auth/token` will need a prior signup/login step. For now, the spec assumes anonymous device auth: first `POST /auth/token` with a new `deviceId` creates a `users` row via upsert; there's no separate registration endpoint.

## 9. Scale considerations (100k concurrent users)

The architecture pattern (fast/safe lane split) holds at this scale — these are infrastructure sizing gaps to close before trusting it under real load, not redesigns.

**Redis** — 3 instances: 1 primary + 2 read replicas
- Single Redis instance is a bottleneck and SPOF at this scale — resolved by running 3: the primary handles all writes (`XADD`, consumer group acks), the 2 replicas provide read capacity and automatic failover if the primary goes down.
- Add client-side jitter to seq_id-replay-on-reconnect logic (random 0-2s delay) so a mass reconnect event (e.g. after a network blip during high-impact news) doesn't send 100k simultaneous replay requests to Redis at once.

**Centrifugo**
- 3 nodes is a starting point, not a sized number — test actual connections-per-node capacity (rough budget: 20-50KB memory per idle WS connection) and scale node count to your Railway instance memory limits.
- Confirm load balancer strategy: connections should distribute evenly across nodes at connect-time; Centrifugo's Redis Engine handles cross-node broadcast so clients on any node still get every message on their subscribed channel.

**Postgres**
- Add connection pooling (PgBouncer, or Railway's managed pooling) between `persistence-worker` and Postgres. Without it, scaling worker replicas will exhaust Postgres's default connection limit (~100) quickly.
- `persistence-worker` should scale horizontally (multiple replicas consuming the same BullMQ queue) independently of `broadcast-worker` — safe lane throughput and fast lane throughput are different problems.

**Load testing before launch**
- Simulate a high-impact news spike specifically (not just steady-state 100k connections) — that's the actual stress case: near-simultaneous broadcast to all subscribers of one channel, plus mass client-side replay requests if any connections dropped.
- **Recommendation (not fully resolved — pick at kickoff): k6.** It has native WebSocket support (`k6/ws` / `k6/experimental/websockets`) and scripts scenarios in JS, which fits a team already writing Node — Artillery works too but k6's scripting model is a better match for simulating "connect, subscribe, hold connection, assert message delivery on spike" than Artillery's engine-config style. Target the WebSocket fan-out path specifically, since that's where 100k concurrent users actually stresses the system — not the webhook or REST endpoints.

## 10. Production gaps — resolve before launch

These aren't covered by sections 1–9. Each one needs actual code, not just config, so they're spelled out at implementation level.

### 10.1 Redis is a single point of failure — resolved: 3 instances

Both pipelines (`broadcast-worker` and `persistence-worker`) would stop entirely if a single Redis instance went down — resolved by running **3 instances: 1 primary + 2 read replicas**, with automatic failover.

- Provision via Railway's Redis plugin (check it supports primary/replica topology) or Upstash (built-in HA, simpler to set up for this exact pattern).
- The primary handles all writes: `XADD` (both webhook and `cot-fetch-worker` publishing), and consumer group offset acks from `broadcast-worker`/`persistence-worker`.
- Replicas serve reads where useful (e.g. the `GET /api/replay` endpoint's `XRANGE` calls, section 10.5) so read load doesn't compete with write throughput on the primary.
- `REDIS_URL` in `.env.example` should become a connection string/config that supports Sentinel (for automatic primary failover detection) — update the ioredis (or equivalent) client config in both workers accordingly, not the plain single-host `Redis()` constructor.
- On primary failover, both workers need reconnect logic that re-resolves the new primary rather than caching the original connection indefinitely — confirm whichever Redis client library is chosen handles this (ioredis does, with Sentinel support built in).

### 10.2 No dead-letter recovery path

`failed_writes` (section 5) is currently write-only — nothing reads from it. Rows land there and stay there forever.

Add to `persistence-worker`:
- A new script/route: `scripts/replay-failed-writes.ts` — reads rows from `failed_writes`, re-enqueues each `payload` back onto `postgres-write-queue` (section 4) as a fresh BullMQ job, and deletes the row on successful re-insert.
- Run manually for now (`npm run replay-failed -- --limit 50`); a cron/scheduled retry can come later once volume is known.
- `failed_writes.retry_count` should increment each time a row is replayed and fails again, so a row that fails repeatedly is visible and can be triaged instead of retried forever blindly.

### 10.3 No monitoring/alerting beyond dead-letter Slack alert

Nothing currently watches for: broadcast-worker falling behind, Centrifugo node health, or the stream growing unbounded because a consumer group stalled.

Minimum viable for launch — each worker should expose a `/health` HTTP endpoint (simple Express route, not through the main webhook routes) returning:
```json
{ "status": "ok" | "degraded", "streamLag": 1234, "lastConsumedAt": "ISO8601" }
```
`streamLag` = difference between the stream's latest entry ID and the consumer group's last-acked ID (`XPENDING` / `XINFO GROUPS`). Wire these into Railway's healthcheck config so a stalled worker gets flagged, not silently stuck.

### 10.4 No stream trimming/retention policy

`stream:market-events` grows unbounded with no cap — this is a real operational risk (Redis memory exhaustion), not a nice-to-have fix.

- Add `MAXLEN ~ 100000` (approximate trimming, cheaper than exact) to every `XADD` call in `ingest-service`/`calendar-webhook`/`cot-fetch-worker`. Tune the number once real event volume is known — 100k is a placeholder, not a load-tested figure.
- Document this cap in `packages/shared-types` alongside the stream key constant so it isn't duplicated/drifted across services.

### 10.5 No replay/backfill mechanism for reconnecting clients

Section 9 mentions "seq_id-replay-on-reconnect logic" exists client-side, but there's no server endpoint for it — this needs to be built, it isn't implied by anything else in the spec.

Add:
```
GET /api/replay?channel=market:euro_fx&sinceSeqId=10200
Response 200: { "messages": [ /* array of envelope-shaped messages, section 2.4 */ ] }
```
- Backed by Redis Stream's own history (`XRANGE`) for recent gaps (covers the "network blip during a news spike" case from section 9) — cap the response (e.g. max 500 messages or reject if `sinceSeqId` is older than the stream's trim window).
- If `sinceSeqId` predates what's still in the trimmed stream, fall back to Postgres (`calendar_events`/`cot_data`, ordered by `seq_id`) for the same channel — same response shape either way, client shouldn't need to know which store answered.
- This endpoint sits in `ingest-service` (already has DB/Redis access patterns established) rather than a new service.

### 10.6 Idempotency is only specified for the calendar webhook

The dedup pattern in section 2.2 (`eventId` + `releaseTime` uniqueness) is specific to that one source. Any new source added later (per the "future data sources" discussion) needs its own explicit dedup key — this won't be inherited automatically.

- When scaffolding a new ingest route, always define: (a) what combination of fields makes an event unique, (b) a `UNIQUE` constraint on that combination in its Postgres table, (c) the same "return 200 + `duplicate: true`" idempotent-response pattern as section 2.2, so upstream retry logic never treats a dedup as a hard failure.

### 10.7 No HMAC secret rotation strategy

`EA_WEBHOOK_HMAC_SECRET` (section 7) has no rotation path — swapping it requires simultaneous redeploy of the EA and the webhook, which isn't realistic.

- Support two secrets during rotation: `EA_WEBHOOK_HMAC_SECRET` (current) and `EA_WEBHOOK_HMAC_SECRET_PREVIOUS` (optional, unset outside rotation windows). Validate the signature against both if the previous var is set; accept if either matches. Remove the previous var once the EA is confirmed on the new secret.

### 10.8 Redis cache miss storm on `/api/market/:symbol` — resolved in section 2.3

Flagging here too since it's exactly the kind of gap that's easy to miss if only section 2's API contracts are read and section 10 is skimmed separately. Full resolution (cache-aside pattern, per-symbol lock, invalidation-on-write) is in section 2.3 — this entry exists so it shows up in the "gaps addressed" list.

- **Root cause avoided:** without a lock, a cache-key expiry during a high-impact news spike (highest-traffic moment by design) means every concurrent request for that symbol misses cache at once and queries Postgres simultaneously — the classic stampede.
- **Fix summary:** `SET NX PX` per-symbol lock in `ingest-service` bounds concurrent Postgres load to 1 query per symbol per refill, regardless of request volume. See section 2.3 for the full read-path logic and why lock-based beats jitter-only here.

This spec + the diagram together give Cursor: what services exist, what they talk over, what the data looks like at every hop, and what happens on failure — plus, as of section 10, what's still missing before this is production-ready. That's enough to scaffold real code from — the diagram alone wasn't.
