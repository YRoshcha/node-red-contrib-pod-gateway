'use strict'

// node-redis stops reconnecting forever the moment reconnectStrategy returns
// an Error instead of a number. That is actually the behaviour callers rely
// on while a client has never connected yet: bin/pod-gateway.js, the
// gateway-server-config startup path and the opt-in Redis integration tests
// all expect a fast, clean rejection when Redis is simply not reachable at
// boot (see test/redis.integration.test.js's `t.skip` on connect failure).
//
// The actual bug is what happens AFTER a successful connection: a later
// Redis restart, Sentinel failover or brief network blip must not
// permanently kill an already-running gateway. So this strategy fails fast
// up to maxInitialRetries while the client has never been ready, and
// switches to unbounded capped backoff once markReady() has been called --
// wire that to the client's 'ready' event via attachErrorLogger's `strategy`
// option below.
const DEFAULT_MAX_DELAY_MS = 5000
const DEFAULT_STEP_MS = 200
const DEFAULT_MAX_INITIAL_RETRIES = 2

function createReconnectStrategy ({ maxDelayMs = DEFAULT_MAX_DELAY_MS, stepMs = DEFAULT_STEP_MS, maxInitialRetries = DEFAULT_MAX_INITIAL_RETRIES } = {}) {
  let everReady = false
  const strategy = retries => {
    if (!everReady && retries > maxInitialRetries) {
      return new Error('Redis connection failed')
    }
    return Math.min(Math.max(0, retries) * stepMs, maxDelayMs)
  }
  strategy.markReady = () => { everReady = true }
  return strategy
}

// node-redis emits one 'error' event per failed reconnect attempt. Logging
// every one of those during a real outage would flood the log, so log the
// first one immediately and then at most once per throttleMs until the
// client is ready again, then log a single recovery line. When `strategy`
// (from createReconnectStrategy) is given, its markReady() is called on the
// client's first 'ready' event so later outages retry forever instead of
// being given up on.
function attachErrorLogger (client, logger, label, { throttleMs = 30000, strategy } = {}) {
  const sink = logger || console
  let lastLoggedAt = 0
  let down = false
  client.on('error', error => {
    down = true
    const now = Date.now()
    if (now - lastLoggedAt >= throttleMs) {
      lastLoggedAt = now
      sink.error?.(`[pod-gateway] ${label} connection error, retrying: ${error?.message || error}`)
    }
  })
  client.on('ready', () => {
    strategy?.markReady?.()
    if (down) {
      sink.info?.(`[pod-gateway] ${label} connection restored`)
      down = false
    }
  })
  return client
}

module.exports = { createReconnectStrategy, attachErrorLogger }
