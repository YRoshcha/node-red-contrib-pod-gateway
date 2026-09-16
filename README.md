# @yroshcha/node-red-contrib-pod-gateway

Node-RED nodes for sending work through one central WebSocket gateway. Every
POD keeps a persistent WebSocket connection to the gateway; only the gateway
calls external APIs and consumes the shared Redis rate-limit budget.

> **Redis is required.** The central Gateway does not start without a reachable
> Redis 6.2+ instance. Redis Streams, the global rate limiter, idempotency,
> delayed retries and pending-result replay all use Redis; MQTT is not a
> replacement for it.

The package is designed for one gateway instance and many POD connections:

```text
POD 1 ──┐
        ├── WebSocket ──► Gateway ──► Redis Streams + GCRA ──► upstream API
POD 2 ──┘                         │
                                  └── result back to the originating POD
```

MQTT is not required. A POD never receives upstream credentials and never
calls the provider API directly.

## What it provides

- operation- or service-level global rate-limit buckets, shared by all PODs;
- a durable Redis Streams queue with `normal` and `bulk` priorities;
- successful Stream-entry deletion and durable delayed retries without repeated
  `XADD` calls;
- synchronous `Gateway Call` and asynchronous `Gateway Out` → `Gateway In`;
- result routing back to the same WebSocket connector that submitted the work;
- a `Gateway Metrics` output node for forwarding lifecycle and latency events;
- idempotency and duplicate-result fan-out;
- retry-aware upstream HTTP errors and request deadlines;
- reconnect, pending-result replay and consumer recovery after a gateway restart;
- optional verbose lifecycle logs that never print payloads or API keys.

Release notes are tracked in [`CHANGELOG.md`](CHANGELOG.md).

## Quick start: the public demo

The fastest way to see the complete flow is to run one Node-RED instance that
contains both the central gateway and a local POD connector.

Requirements: Node.js 18+, Node-RED, and **Redis 6.2+ (mandatory)**. Redis
Streams and the `XAUTOCLAIM` command are used by the Gateway queue.

1. Install the palette in Node-RED through **Manage palette → Install**, or
   install it in the Node-RED runtime:

   ```bash
   npm install @yroshcha/node-red-contrib-pod-gateway
   ```

2. Start Redis. For a local Docker setup:

   ```bash
   docker compose up -d redis
   ```

3. Import [`examples/public-demo-flow.json`](examples/public-demo-flow.json)
   into Node-RED.

4. Open **Central Gateway + Redis** and set `Redis URL`. With Redis on the
   same host use `redis://127.0.0.1:6379`; with a Compose service named
   `redis` use `redis://redis:6379`.

5. Open the **API Ninjas** config node and enter an API key in its credential
   field. The adapter calls
   `GET https://api.api-ninjas.com/v1/worldtime?timezone=Europe/Kyiv` with the
   key in `X-Api-Key`.

6. Deploy and click **Call API Ninjas**. The first branch returns the result
   to `Gateway Call`. The result branch of the async example first reports
   `accepted`, then `Gateway In` receives the completed response.

The public flow contains no secret. Credentials are entered in Node-RED and
are not stored in the flow export.

### Docker URL rule

`127.0.0.1` always means “this container”. If Node-RED and Redis are separate
containers, use the Redis service DNS name. If the gateway is in another
container, POD-side `Gateway Config` must use the gateway service DNS name,
for example `ws://gateway:8080/ws`.

## The Node-RED model

### Central gateway (one instance)

Create one **Gateway Server Config** node. It owns the WebSocket listener,
Redis queue, global rate limiter and gateway authentication.

> **Run exactly one Gateway Server Config instance. Horizontal scaling of the
> gateway itself is not supported yet.** The Redis Streams queue is
> multi-consumer-ready (a consumer group lets a second reader reclaim work
> after a crash), but each POD's live WebSocket connection is held only in
> the memory of the gateway process it happens to be connected to. A second
> gateway replica could dequeue a request that originated on a POD connected
> to the *first* replica, with no socket to deliver the result on -- that
> request would only reach the POD later, through the disconnected-POD
> pending-result/replay path, not immediately. Do not run more than one
> replica in Kubernetes or behind a load balancer; use a single replica with
> automatic restart (the `/healthz` endpoint reports `queueReady` and
> `rateLimiterReady` so a liveness probe can restart the process if its Redis
> connections are down) for availability instead of horizontal scale-out.

Create one **Gateway API Config** per provider. It stores the provider base
URL, common headers and API key in Node-RED credentials.

A header value on **Gateway API Config** or **Gateway Adapter** may use the
same `{{...}}` template syntax as `_request.headers` (see below), plus a
`global` root for Node-RED's global context: `{{global.apiToken}}`,
`{{global.tenant.id}}`. This is the place for a credential that a separate
flow refreshes into global context, since these headers are operator config,
not POD input -- there is no protected-header restriction here the way there
is for `_request.headers`. `{{payload.x}}` and `{{request.x}}` also work,
resolved per request from the same call that resolves `_request.headers`. A
header with no `{{...}}` is unaffected and stays static, resolved once at
deploy, exactly as before. A missing or non-scalar `{{global.x}}` value fails
that call with `INVALID_HEADER_TEMPLATE` (non-retryable).

The main use case is `Authorization` itself. It is unconditionally on the
protected-header list (see below), so a POD's `_request.headers` can never
carry it -- an upstream bearer token has to come from the API Config or
Adapter. `{{global.apiToken}}` is what makes that workable when the token is
short-lived: keep a separate flow (e.g. an OAuth refresh timer) writing the
current token to global context, and set the **Gateway API Config** header
to:

```
Authorization: Bearer {{global.apiToken}}
```

It is re-read from global context on every request, so a refreshed token
takes effect on the next call with no redeploy of the Gateway Adapter.

**Header priority**, highest first, for a header name set at more than one
level: `_request.headers` (POD) > **Gateway Adapter** headers > **Gateway
API Config** headers. This holds whether or not either config-level value
uses a `{{...}}` template -- a template is just resolved before the merge,
then the same last-value-wins order applies. The one exception is any
protected/credential header (`Authorization`, `Cookie`, `Host`,
`Content-Length`, `Proxy-Authorization`, `X-Api-Key`, `X-Auth-Token`,
`X-Rapidapi-Key`, `Api-Key`, or a custom `apiKeyHeader`): those names are
never settable from `_request.headers`, by design, so for them the priority
is only Adapter > API Config, with no POD override at any priority level.

Create one **Gateway Adapter** per operation. It reuses an API config and only
needs an operation key, path and HTTP method:

```text
Gateway API Config: https://api.api-ninjas.com
Gateway Adapter:
  operation = worldtime/getKyivTime
  path      = /v1/worldtime?timezone=Europe/Kyiv
  method    = GET
```

The API path may contain values from the incoming payload or the standard
request object. Use `{{payload.id}}`, `{{request.params.id}}` (or the
equivalent `{{msg.payload.id}}` / `{{msg._request.params.id}}`) and the gateway
resolves it for every request:

```text
/v1/users/{{payload.id}}?region={{payload.region}}
```

Template values are URL-encoded automatically. `{{gateway.requestId}}` can be
used when the upstream API accepts a request identifier. A missing or
non-scalar value fails with `INVALID_URL_TEMPLATE`; it is never replaced with
an empty string. The request payload itself is still passed to the adapter
unchanged.

Each **Gateway Adapter** also has a **Keep-Alive** checkbox (on by default).
Node's `fetch` already reuses persistent HTTP/1.1 connections to the same
upstream host between requests, so leaving it checked changes nothing.
Uncheck it only for a provider or load balancer that mishandles connection
reuse; the gateway then sends `Connection: close` on every request for that
operation, and a POD cannot turn it back on through `_request.headers`.

### Standard request contract

`Gateway Call` and `Gateway Out` accept an optional `msg._request` object. The
API URL and credentials stay in the central API/Adapter configuration; a POD
can provide only per-request values:

```js
msg.payload = { id: 'user-42', active: false }
msg._request = {
  params: { id: 'user-42' },       // values for {{request.params.*}} in the path
  query: { include: 'profile' },   // appended to the configured API path
  headers: {
    'X-Correlation-ID': msg._correlationId,
    'X-Tenant-ID': '{{payload.tenantId}}',
    'X-Request-ID': '{{gateway.requestId}}'
  },
  body: { active: false }           // used by POST, PUT, PATCH and DELETE
}
return msg
```

`params`, `query` and `headers` must be objects with scalar values (query
arrays are repeated); API credentials and transport headers such as
`Authorization`, `Cookie` and `Host` remain controlled by **Gateway API
Config**. Header values may be resolved per request with the same template
syntax as the path, for example `{{payload.tenantId}}`,
`Bearer {{payload.accessToken}}` or `{{gateway.requestId}}`. Header templates
are sent as-is (they are not URL-encoded); missing values and line breaks are
rejected before the request is queued. Protected credential/transport headers
cannot be overridden from a POD. `GET` and `HEAD` operations are query-only. `POST`, `PUT`, `PATCH`
and `DELETE` use `_request.body`; when it is absent, the original
`msg.payload` is sent as JSON. `UPDATE` is not an HTTP method—use `PUT` for a
replacement or `PATCH` for a partial update.

The same contract is validated in the POD node and again by the central
Gateway before a task enters Redis. Invalid fields fail immediately with
`REQUEST_VALIDATION_FAILED`; no upstream request is made.

Every completed call returns the API result in `msg.payload` and a canonical
input/output envelope:

```js
msg._request = {
  input: {
    method: 'PUT',
    params: { id: 'user-42' },
    query: { include: 'profile' },
    headers: { 'X-Correlation-ID': 'corr-1' },
    body: { active: false },
    payload: { id: 'user-42', active: false }
  },
  output: {
    statusCode: 200,
    body: { updated: true }
  }
}
msg.gateway.httpStatus = 200
```

For an upstream error, `output.statusCode` contains the provider status and
`output.body` contains its response body when one was returned;
`output.error` contains the normalized gateway error. `Gateway In` forwards the
same envelope for asynchronous results.

The operation must use `service/name`. Rate-limit rules are resolved in this
order: exact operation (`service/name`), then the `service` prefix, then the
`default` rule. If only the service rule exists, all operations under that
service share one budget.

If one provider exposes many endpoints, keep the same service prefix and define
the limit once. For example, these 20 operations all share one Redis bucket:

```text
weather/getKyivCurrent
weather/getForecast
weather/getAirQuality
...any other weather/* operation
```

The matching **Rate limits JSON** is only:

```json
{
  "weather": { "rate": 10, "burst": 2 }
}
```

`rate` is the sustained requests-per-second budget and `burst` is the allowed
short burst. For independent endpoint budgets under one provider, use exact
operation keys:

```json
{
  "user/getInfo": { "rate": 20, "burst": 20 },
  "user/update": { "rate": 1, "burst": 1 }
}
```

These rules create separate Redis buckets
`pod-gateway:{user/getInfo}:g` and `pod-gateway:{user/update}:g`.

You can also set the limit directly on each **Gateway Adapter** with its
**Rate limit**, **per second/per minute** and **Burst** fields. A value entered
there becomes an exact operation rule and overrides the matching JSON rule. The
per-minute choice is converted to requests per second before the Redis GCRA
check. For example, set `20` + `per second` + burst `20` on `user/getInfo`,
and `1` + `per second` + burst `1` on `user/update`. Burst cannot be smaller
than the entered Rate limit; the editor raises the minimum automatically. Leave
the Rate limit field empty to use the central JSON configuration instead.

### POD-side flow

Create one **Gateway Config** per POD runtime and reuse it from all functional
nodes. It contains only the WebSocket URL, POD ID, optional gateway token and
reconnect settings.

| Node | Use it for | Outputs |
| --- | --- | --- |
| **Gateway Call** | request/response work | output 1: result, output 2: error |
| **Gateway Out** | fire-and-forget or long async work | output 1: accepted, output 2: send error |
| **Gateway In** | result/event delivery for `Gateway Out` | one message output |

The **Gateway Call** editor has an explicit **Adapter / operation** dropdown.
It is populated from the gateway's registered capabilities. Selecting an
adapter also shows its contract: HTTP method, path template and whether
`params`, `query`, `headers` and `body` are accepted. If the gateway is
temporarily unavailable, the current operation can still be entered manually.
`Gateway Out` and `Gateway In` expose the same operation picker. The
**Gateway Adapter** editor keeps operation entry manual because it is where a
new adapter is created; its separate **HTTP method** dropdown contains
`GET`, `POST`, `PUT`, `PATCH`, `DELETE` and `HEAD`.

The common shapes are:

```text
[Inject] ─► [Gateway Call] ─► [result]
                         └────► [error]

[Inject] ─► [Gateway Out] ─► [accepted]

[Gateway In] ─► [completed async result]
```

`Gateway Call` waits over the WebSocket; it does not create a one-minute HTTP
request between the POD and gateway. `Gateway Out` resolves as soon as Redis
accepts the task. Its eventual `result` is delivered to every `Gateway In`
node using the same `Gateway Config` connection and matching operation filter.

### Gateway metrics output

Add one **Gateway Metrics** node next to the central **Gateway Server Config**
and connect its output to any metrics palette. It emits metadata-only messages
for the Gateway lifecycle; no payloads, headers or credentials are forwarded.
Leave the event filter empty for all events, or enter a comma-separated list
such as `request.completed, upstream.completed`.

Available events are `request.received`, `request.accepted`,
`request.processing`, `request.completed`, `request.rejected`,
`request.duplicate`, `upstream.started`, `upstream.completed` and
`rate_limit.decision`.

For a completed request, `msg.payload` has this shape:

```json
{
  "event": "request.completed",
  "service": "worldtime",
  "operation": "getKyivTime",
  "outcome": "success",
  "httpStatus": 200,
  "durationMs": 1347,
  "timings": {
    "queueMs": 210,
    "rateLimitMs": 12,
    "upstreamMs": 980,
    "deliveryMs": 5,
    "totalMs": 1347
  }
}
```

Use `timings.upstreamMs` for the external API latency and
`timings.totalMs` for the end-to-end Gateway latency. The node's `msg.topic`
is `pod-gateway/<event>`, and the same object is also available as
`msg.metric` for palettes that use a dedicated metric field.
The `upstream.completed` event is an attempt-level event and exposes its
latency as `durationMs` instead of `timings`; select `request.completed` when
you need both upstream and end-to-end timings in one message.

#### Prometheus with `@yroshcha/node-red-contrib-nodered-metrics`

The Gateway package does not depend on a specific metrics implementation. If
you use [`@yroshcha/node-red-contrib-nodered-metrics`](https://flows.nodered.org/node/@yroshcha/node-red-contrib-nodered-metrics), install that palette separately and import [`examples/metrics-flow.json`](examples/metrics-flow.json). The example already contains its `nodered-metric-config`, `nodered-metric` and `nodered-metrics-exporter` nodes:

- `nodered_gateway_requests_total{service,operation,outcome}` counts completed
  Gateway requests and creates a companion total-duration histogram;
- `nodered_gateway_upstream_duration_seconds{service,operation,outcome}`
  observes each external API attempt in seconds;
- the exporter exposes the registry at `/nodeRedMetrics`.

Prometheus can scrape the central Node-RED instance with:

```yaml
scrape_configs:
  - job_name: pod-gateway
    metrics_path: /nodeRedMetrics
    static_configs:
      - targets: ["gateway:1880"]
```

Workers do not need the metrics palette when all external API calls run through
the central Gateway.

## Redis configuration

Redis is configured in **Gateway Server Config**, not in the POD-side
**Gateway Config** node. Use one stable key prefix per gateway environment and
one stable consumer name for the single gateway instance.

Typical settings:

```text
Redis URL:          redis://redis:6379
Redis key prefix:   pod-gateway
Redis consumer:     production-gateway
Max queue size:     10000
Rate limits JSON:   {"upstream":{"rate":50,"burst":1}}
```

For TLS Redis use a `rediss://` URL, for example:

```text
rediss://user:password@redis.example.com:6379
```

For a Redis TLS config like:

```json
{
  "host": "redis.example.com",
  "port": 6379,
  "tls-port": 6379,
  "tls": { "rejectUnauthorized": false }
}
```

set these fields in **Gateway Server Config**:

```text
Redis URL:                       rediss://redis.example.com:6379
Verify Redis TLS certificate:    disabled
```

The `tls-port` value is expressed by the `rediss://` scheme. If the Redis
deployment requires authentication, add the credentials to the URL, for
example `rediss://:password@redis.example.com:6379`.

Keep **Verify Redis TLS certificate** enabled in production. Disable it only
when certificate verification is intentionally handled by another trusted
layer.

The queue uses these readable keys (with prefix `pod-gateway`):

```text
pod-gateway:queue:normal
pod-gateway:queue:bulk
pod-gateway:queue:depth
pod-gateway:retry:normal    # delayed retries, sorted by due timestamp
pod-gateway:retry:bulk
pod-gateway:retry:data:normal # payloads for delayed retry tokens
pod-gateway:retry:data:bulk
pod-gateway:{bucket}:g        # service or exact operation bucket
# examples: pod-gateway:{upstream}:g, pod-gateway:{user/getInfo}:g
```

If Redis still shows `<prefix>:{queue}:normal` or `<prefix>:{queue}:bulk`, those
are legacy Stream names from an older build. The `{queue}` part was a Redis
Cluster hash tag, not a variable that should be expanded. Current builds use
`<prefix>:queue:normal` and `<prefix>:queue:bulk`; remove legacy keys only after
checking that they contain no pending work.

Redis Streams are acknowledged only after the adapter finishes and the result
has been delivered or persisted for replay. With **Delete processed Stream
entries** enabled (the default), the gateway then removes the acknowledged
entry with `XDEL`, so successful history does not accumulate in Redis. Disable
that option only for short debugging retention.

Rate-limited and retryable work is kept once in the `retry:normal` or
`retry:bulk` sorted set with its due timestamp. A small gateway retry loop
promotes it back to the Stream only when due; it does not append a new Stream
entry every second. The queue depth slot is transferred across the delayed
state, so `Max queue size` still bounds total live work. A pending Stream item
can be reclaimed after a crashed gateway. Historical consumer names may remain
in Redis after redeploys; a stable `Redis consumer` value prevents new names
from being created on every deploy.

**Idempotency TTL (s)** controls the expiration of Redis idempotency claims,
completed idempotency results and disconnected-POD pending results. Set it
longer than the period in which a duplicate request must be recognized; the
default is 3600 seconds. The same values are available in the standalone JSON
configuration as `redis.resultTtlSeconds`, `redis.deleteOnAck` and
`redis.retryPollMs`.

A pending Stream entry is reclaimed by `XAUTOCLAIM` after `claimIdleMs`
(default 120s). This is not an editor field: Gateway Server Config derives it
automatically as `max(120000, Upstream timeout (ms) + 30000)`, so a slow
provider timeout never falls inside the reclaim window and a still-running
call cannot be dispatched a second time. If you set a longer per-request
`upstreamTimeoutMs` in `msg._request` than the configured default, or run the
standalone JS gateway with its own `claimIdleMs`, keep the same margin
yourself; the gateway logs a `upstream timeout ... exceeds the Redis Stream
claimIdleMs ...` warning (once per operation) when it detects the mismatch at
runtime.

## Authentication and security

- Use `wss://` behind a TLS reverse proxy in production.
- Set a gateway token in **Gateway Server Config** and the matching credential
  in each POD-side **Gateway Config**.
- Keep provider API keys only in **Gateway API Config** credentials or gateway
  process environment variables.
- Do not put secrets in an exported flow, `msg.payload`, operation names or
  debug nodes.
- `Verbose console logging` is intended for development. It logs request IDs,
  queue IDs and lifecycle state, never payloads, headers or credentials.

## Error handling

Errors use a stable shape:

```json
{
  "code": "UPSTREAM_HTTP_429",
  "message": "External API returned HTTP 429",
  "retryable": true,
  "retryAfterMs": 2000
}
```

Useful codes include:

| Code | Meaning |
| --- | --- |
| `GATEWAY_CONNECT_TIMEOUT` | POD could not complete the WebSocket handshake |
| `GATEWAY_ACCEPT_TIMEOUT` | Gateway did not acknowledge the task |
| `GATEWAY_RESULT_TIMEOUT` | Task was accepted but no result arrived before the POD deadline |
| `QUEUE_FULL` | Redis queue reached `Max queue size` |
| `RATE_LIMITER_UNAVAILABLE` | Redis limiter could not make a decision |
| `UPSTREAM_TIMEOUT` | The gateway aborted a slow provider request |
| `UPSTREAM_NETWORK_ERROR` | Provider DNS/TCP/TLS request failed |
| `UPSTREAM_HTTP_4xx/5xx` | Provider returned a non-2xx response |
| `REQUEST_VALIDATION_FAILED` | `_request` does not match the configured API method/path contract |
| `OPERATION_NOT_FOUND` | No adapter is registered for `service/name` |

For a failed `Gateway Call`, output 2 contains the original message plus
`msg.error` and `msg.gateway.status = "failed"`. For an async operation,
`Gateway Out` reports acceptance on output 1; inspect errors from the matching
`Gateway In` path.

## Importable examples

- [`public-demo-flow.json`](examples/public-demo-flow.json) — minimal real
  external API call; configure an API Ninjas key.
- [`external-api-flow.json`](examples/external-api-flow.json) — one-tab flow
  showing both `Gateway Call` and `Gateway Out` → `Gateway In`.
- [`metrics-flow.json`](examples/metrics-flow.json) — central Gateway metrics
  output with a free Open-Meteo Kyiv weather call, Prometheus nodes from
  `@yroshcha/node-red-contrib-nodered-metrics` and debug outputs.
- [`full-demo-flow.json`](examples/full-demo-flow.json) — one-tab local demo
  with a fake upstream HTTP endpoint; no external API account is required.
- [`retention-retry-flow.json`](examples/retention-retry-flow.json) — one-tab
  dev.29 demo for Redis XDEL retention, durable delayed retries, burst calls
  and Gateway latency metrics.
- [`custom-headers-flow.json`](examples/custom-headers-flow.json) — one-tab
  local GET/POST demo that echoes static and dynamic custom headers received by
  the upstream endpoint.
- [`local-flow.json`](examples/local-flow.json) — POD-only flow for the
  standalone `examples/local-gateway.js` process.
- [`central-gateway-flow.json`](examples/central-gateway-flow.json) — central
  server, provider config and adapter only.
- [`basic-flow.json`](examples/basic-flow.json) — smallest POD-side call.

All files are real Node-RED export arrays and can be imported directly.

If the metrics example reports `UPSTREAM_HTTP_404`, remove the old imported
adapter and import the current `metrics-flow.json` again. The adapter path must
be exactly `/v1/forecast?...` and the API base URL must be
`https://api.open-meteo.com`; existing Node-RED nodes are not changed when a
new package archive is installed.

## Standalone JavaScript gateway

The package also exports `GatewayServer`, `GatewayClient`,
`RedisGcraRateLimiter`, `RedisStreamQueue`, `createJsonHttpAdapter` and the
`requestContract` helpers for an application that does not host the gateway in
Node-RED.

```js
const {
  GatewayServer,
  RedisGcraRateLimiter,
  RedisStreamQueue,
  createJsonHttpAdapter,
  requestContract,
  loadGatewayConfig
} = require('@yroshcha/node-red-contrib-pod-gateway')

const config = loadGatewayConfig()
const limiter = await RedisGcraRateLimiter.connect({
  url: config.redis.url,
  keyPrefix: config.redis.keyPrefix,
  limits: config.rateLimits
})
const queue = await RedisStreamQueue.connect({
  url: config.redis.url,
  prefix: config.redis.keyPrefix,
  maxQueueSize: config.redis.maxQueueSize,
  resultTtlSeconds: config.redis.resultTtlSeconds,
  deleteOnAck: config.redis.deleteOnAck,
  retryPollMs: config.redis.retryPollMs
})
const gateway = new GatewayServer({
  port: config.server.port,
  queue,
  rateLimiter: limiter,
  authenticate: async message => !config.auth.token || message.token === config.auth.token
})

gateway.registerOperation('worldtime/getKyivTime', createJsonHttpAdapter({
  url: 'https://api.api-ninjas.com/v1/worldtime?timezone=Europe/Kyiv',
  method: 'GET',
  headers: { 'X-Api-Key': process.env.API_NINJAS_KEY }
}), {
  label: 'World time',
  requestSchema: requestContract.requestSchema('GET', '/v1/worldtime?timezone=Europe/Kyiv')
})

await gateway.start()
```

For a local standalone run:

```bash
cp gateway.config.example.json gateway.config.json
docker compose up -d redis
node examples/local-gateway.js
```

Then import [`examples/local-flow.json`](examples/local-flow.json) and use
`ws://127.0.0.1:8080/ws`.

## Configuration file and environment overrides

The standalone command reads `gateway.config.json`; copy the safe template:

```bash
cp gateway.config.example.json gateway.config.json
npx pod-gateway
```

The file is ignored by npm/git examples because it may contain credentials.
Deployment variables override file values:

```text
REDIS_URL
REDIS_KEY_PREFIX
GATEWAY_MAX_QUEUE
PORT
GATEWAY_CONCURRENCY
GATEWAY_TOKEN
GATEWAY_RATE_LIMITS   # JSON object
REDIS_RESULT_TTL_SECONDS
REDIS_DELETE_ON_ACK
REDIS_RETRY_POLL_MS
```

### Real Gateway load test

`npm run test:load` starts a real `GatewayServer`, a real Redis Streams queue
and a real WebSocket client, then reports completion rate, throughput, queue
depth, delayed-retry depth and Redis `used_memory` before/after the run. The
adapter is local and deterministic, so no provider API key or external API is
needed.

The default smoke run sends 200 requests at 200 requests/second. To model the
long-running 1 request/second budget with 50,000 incoming requests, run:

```bash
LOAD_REQUESTS=50000 LOAD_RATE=1 LOAD_LIMIT=1 npm run test:load
```

That scenario intentionally takes about 13 hours 53 minutes when every call
must pass through a 1 RPS upstream budget. Use a smaller `LOAD_REQUESTS` first
to validate connectivity. `LOAD_LIMIT=0` disables the limiter for a fast queue
and retention/memory smoke test.

## Troubleshooting

1. Check `http://<gateway-host>:8080/healthz`. A healthy gateway returns
   `{"ok":true,...}`, plus `rateLimiterReady`/`queueReady` when a rate limiter
   or Redis queue is configured -- either turning `false` is why `ok` went
   `false` even though the process is still running (e.g. a liveness probe
   restarting it makes sense once Redis is unreachable, not before).
2. If a POD is `disconnected`, verify the WebSocket URL from that POD's
   network namespace. `127.0.0.1` is usually the wrong address across
   containers.
3. If a call is `accepted` without a queue ID, verify the installed package
   version and restart Node-RED completely; the queue path must log `redis
   enqueue`.
4. If RedisInsight shows a consumer group with zero entries, send a new task
   and look for `redis enqueue`, `redis dispatch` and `redis ACK` logs.
5. Temporarily enable verbose logging in both server and POD config nodes.
   Disable it again after diagnosis.

Node-RED stores the value entered in each existing **Gateway Server Config**
node. Installing a new palette archive does not rewrite that node. If an old
Redis endpoint appears in the editor, open that config node and replace its
value, or delete the old config node before importing a fresh example flow.

## Development and release

```bash
npm install
npm test
npm pack --dry-run
npm pack
```

The test suite covers protocol validation, URL/header handling, upstream HTTP
mapping, in-memory and Redis GCRA decisions, Redis Streams enqueue/dispatch/
ACK/idempotency/pending replay, gateway server routing, WebSocket client
timeouts and duplicate aliases, helper functions, and every importable flow.

The real Redis end-to-end test is opt-in by availability: it uses
`REDIS_URL` when set and skips cleanly when Redis is not reachable.

Before publishing a stable release, update the version in `package.json` and
`package-lock.json`, run the commands above, inspect `npm pack --dry-run`, and
publish the generated package from a clean release environment:

```bash
npm publish --access public
```
