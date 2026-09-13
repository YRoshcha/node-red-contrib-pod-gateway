# Changelog

## 1.0.0 — 2026-09-10

First stable release: a central WebSocket gateway for Node-RED PODs, backed by
Redis for durable queuing, global rate limiting, idempotency and delayed
retries.

**Gateway core**

- one central Gateway Server Config owns the WebSocket listener, Redis queue,
  global rate limiter and gateway authentication; PODs connect over a
  persistent WebSocket and never call upstream APIs or hold provider
  credentials directly;
- `Gateway Call` (synchronous request/response) and `Gateway Out` → `Gateway
  In` (fire-and-forget / long async work), with results routed back to the
  same connection that submitted the work, pending-result replay after a POD
  reconnect, and duplicate-result fan-out for shared idempotency keys;
- a standard `msg._request` input/output envelope (`params`, `query`,
  `headers`, `body`) validated against each adapter's configured HTTP
  method/path contract, both on the POD and again by the central gateway
  before a task enters Redis;
- per-request dynamic header templates and URL templates in adapter paths,
  with scalar validation, line-break protection and automatic URL-encoding;
  protected credential/transport headers (a static list plus any custom
  `apiKeyHeader` configured on a Gateway API Config) can never be overridden
  by a POD;
- retry-aware upstream HTTP error mapping, request deadlines, and reconnect
  jitter (+/-30%) on the POD side so many PODs reconnecting after a shared
  gateway restart or network blip do not retry in one synchronized burst;
- a per-adapter **Keep-Alive** checkbox for upstream HTTP calls (on by
  default, matching Node's existing connection-reuse behaviour); unchecking
  it forces `Connection: close` on every request to that operation, which a
  POD cannot override.

**Redis queue and rate limiting**

- a durable Redis Streams queue with `normal`/`bulk` priorities and a
  consumer group, so pending entries can be reclaimed with `XAUTOCLAIM` after
  a crash;
- successful Stream entries are deleted after `XACK` by default (configurable
  retention for debugging);
- rate-limited and retryable work is persisted once in a durable delayed-
  retry sorted set and promoted back to the Stream only when due, instead of
  re-appending a new Stream entry on every tick; the check-then-act sequence
  (already-scheduled check, queue-depth increment, write) runs as one atomic
  Redis Lua script to avoid double-counting queue depth under concurrent
  reschedules;
- a GCRA rate limiter (Lua script) shared by all PODs, with per-operation,
  per-service-prefix and default rules, editable from Gateway Adapter or a
  central JSON configuration;
- idempotency claims and results, and disconnected-POD pending results, all
  backed by Redis with a configurable TTL, plus an atomic in-memory fallback
  when no Redis is configured;
- resilient Redis reconnect handling: the rate limiter and Stream queue
  clients fail fast while never yet connected (surfacing boot-time
  misconfiguration immediately), then retry with capped backoff indefinitely
  once connected, surviving a Redis restart, Sentinel failover or network
  blip without a process restart;
- `claimIdleMs` (the delay before an unacknowledged Stream entry is
  reclaimed) is derived automatically from the configured upstream timeout,
  and a runtime warning is logged once per operation if a longer per-request
  timeout would let a still-running call be reclaimed and re-dispatched.

**Observability and operations**

- `/healthz` reports `ok`, `active`, `queueDepth`, and, when configured,
  `rateLimiterReady` and `queueReady`, so a liveness probe can restart the
  process precisely when its Redis connections are down;
- a `Gateway Metrics` output node emits request/upstream/rate-limit lifecycle
  events with queue, rate-limit, upstream and total latency timings, and
  metadata only — never payloads, headers or credentials; an example
  Prometheus integration is included;
- optional verbose lifecycle logging that never prints payloads or API keys;
- documented that only one Gateway Server Config instance should run at a
  time — the queue is multi-consumer-ready, but a POD's live WebSocket
  connection lives only in the memory of the gateway process it is attached
  to, so horizontal scale-out of the gateway itself is not yet supported.

**Node-RED editor and packaging**

- `Gateway Server Config`, `Gateway API Config`, `Gateway Adapter`, `Gateway
  Config`, `Gateway Call`, `Gateway Out`, `Gateway In` and `Gateway Metrics`
  nodes, with visible Adapter/operation pickers showing the selected
  adapter's method/path/request contract, editable Name fields, and
  per-operation rate-limit fields with per-second/per-minute units and burst
  control;
- importable example flows covering a real external API call, local/fake
  upstream demos, custom headers, Redis retention and delayed retries, and
  Prometheus metrics;
- a standalone JavaScript entry point (`GatewayServer`, `GatewayClient`,
  `RedisGcraRateLimiter`, `RedisStreamQueue`, `createJsonHttpAdapter`,
  `requestContract`) for running the gateway outside Node-RED, with file- and
  environment-variable-based configuration;
- a real Gateway + Redis load-test harness, an opt-in real-Redis integration
  test suite, and unit coverage across protocol, adapters, Redis, WebSocket,
  gateway routing, Node-RED nodes and importable flows.
