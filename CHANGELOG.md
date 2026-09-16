# Changelog

## 2.0.5 — 2026-09-16

Bug fix and a new templating capability, no breaking changes.

- **Fixed:** `msg._request` built as a plain object literal inside a Node-RED
  Function node was incorrectly rejected with `_request must be an object`
  (`REQUEST_VALIDATION_FAILED`). Function nodes run user code in a separate
  vm context/realm, so an object literal created there has a *different*
  `Object.prototype` reference than the main Node-RED process; the previous
  check compared prototypes with strict `===` and failed for any such
  object, regardless of its contents. `isPlainObject` (`lib/request-contract.js`)
  now detects plain objects with `Object.prototype.toString.call(value) ===
  '[object Object]'`, which is realm-independent. A `msg._request` built with
  a **Change** node (JSON/JSONata) was never affected, since those evaluate
  in the main process.
- **Added:** Gateway API Config and Gateway Adapter headers can now use the
  same `{{...}}` template syntax as `_request.headers`, plus a new `global`
  root that reads Node-RED's global context: `{{global.apiToken}}`,
  `{{global.tenant.id}}`. Unlike `_request.headers`, this is operator-authored
  config, so there is no protected-header restriction -- a credential can
  legitimately live behind `{{global.apiToken}}`, refreshed by another flow
  that writes to the same global context key. `{{payload.x}}` and
  `{{request.x}}` also work here, matching `_request.headers`. Headers with
  no template are unaffected and stay static, resolved once at deploy as
  before; a missing or non-scalar `{{global.x}}` value fails the call with
  `INVALID_HEADER_TEMPLATE` (non-retryable) rather than crashing the adapter.
  Header priority for an ordinary (non-credential) header name set at more
  than one level is unchanged and now has explicit test coverage:
  `_request.headers` (POD) > Gateway Adapter > Gateway API Config, template
  or not. Protected/credential header names (`Authorization`, `Cookie`,
  `Host`, a configured `apiKeyHeader`, etc.) remain outside this order --
  `_request.headers` can never set them, at any priority level.

## 2.0.0 — 2026-09-13

Breaking change: node type identifiers renamed.

- all 8 node types renamed from the `gateway-*` prefix to `pod-gateway-*`
  (`gateway-config` -> `pod-gateway-config`, `gateway-server-config` ->
  `pod-gateway-server-config`, `gateway-api-config` -> `pod-gateway-api-config`,
  `gateway-adapter` -> `pod-gateway-adapter`, `gateway-in` -> `pod-gateway-in`,
  `gateway-out` -> `pod-gateway-out`, `gateway-call` -> `pod-gateway-call`,
  `gateway-metrics` -> `pod-gateway-metrics`). The flows.nodered.org scorecard
  flags this package's plain `gateway-*` types as colliding with unrelated
  types registered by `@smappee/node-red-contrib-smappee` and
  `@smappee/node-red-contrib-smappee-knx` (both ship a node literally typed
  `gateway`); the previous 1.0.1 entry below explains why this was initially
  left alone, but a firmer prefix removes the ambiguity for good instead of
  relying on exact-string non-collision.
- **Upgrading from 1.0.x**: any existing flow using these nodes will show
  "unknown node type" after upgrading until it is re-deployed with the new
  node palette. There is no automatic migration -- open each affected tab,
  the renamed nodes will need re-adding (config nodes in particular are
  matched by type, so a `gateway-config` reference cannot resolve to a
  `pod-gateway-config` instance automatically). Given how recent 1.0.0/1.0.1
  are and their minimal adoption, this is done now rather than later once
  more flows depend on the old names.
- module file paths (`nodes/gateway-adapter.js`, etc.) are unchanged; only
  the registered type strings and the `node-red.nodes` map keys in
  package.json moved.

## 1.0.2 — 2026-09-13

License change, no functional or protocol changes.

- relicensed from MIT to Apache-2.0 (`LICENSE` replaced with the full
  Apache License, Version 2.0 text; `package.json` `license` field updated
  to match). Apache-2.0 adds an explicit patent grant and a patent
  retaliation clause on top of what MIT covers; existing MIT-licensed
  copies of 1.0.0/1.0.1 stay valid under their original terms.

## 1.0.1 — 2026-09-13

Metadata/packaging patch, no functional or protocol changes.

- added `repository`, `bugs` and `homepage` to package.json now that the
  source is public on GitHub;
- declared `"node-red": {"version": ">=2.0.0"}` compatibility (verified
  live against Node-RED 5.0.7; no version-specific APIs are used, so the
  floor is set conservatively rather than to the exact tested version);
- bumped the `ws` dependency range to `^8.21.3` (latest 8.x patch; no
  breaking changes). `redis` stays on `^4.7.0` for now -- node-redis 5/6
  changed parts of the client API and deserve their own compatibility pass
  before bumping a major version, not a metadata-only patch.
- Node type names (`gateway-*`) were flagged by the Flow Library scorecard
  as overlapping with other modules that also register a node literally
  called `gateway` (e.g. @smappee/node-red-contrib-smappee-knx). Node-RED
  resolves node types by their exact registered string, so `gateway` and
  `gateway-adapter` are different identifiers and do not collide at
  runtime; renaming our 8 already-published node types now would break
  every flow built against 1.0.0, so this is left as-is.

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
