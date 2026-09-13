'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { GatewayServer } = require('../lib/gateway-server')
const { requestSchema } = require('../lib/request-contract')

function fakeConnection (server, podId = 'pod-test') {
  const sent = []
  const ws = { readyState: 1, send: value => sent.push(JSON.parse(value)), close: () => {} }
  const connection = {
    ws,
    authenticated: true,
    connectionId: `connection-${podId}`,
    podId,
    instanceId: `instance-${podId}`
  }
  server.connections.set(ws, connection)
  server.connectionsByPod.set(podId, connection)
  return { connection, sent }
}

function closeServer (server) {
  return new Promise(resolve => server.wss.close(() => resolve()))
}

test('registers operations and exposes capability metadata', async t => {
  const server = new GatewayServer()
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async () => 'ok', { label: 'Echo', description: 'Echoes a value', inputSchema: { type: 'string' } })
  assert.deepEqual(server.capabilities(), [{
    operation: 'demo/echo',
    label: 'Echo',
    description: 'Echoes a value',
    inputSchema: { type: 'string' }
  }])
  assert.equal(server.unregisterOperation('demo/echo'), true)
  assert.equal(server.unregisterOperation('demo/echo'), false)
  assert.throws(() => server.registerOperation('invalid', () => {}), /service\/name/)
})

test('advertises the request contract and rejects an invalid request before queueing', async t => {
  let enqueued = 0
  const server = new GatewayServer({
    queue: { enqueue: async () => { enqueued++; return { id: 'never' } } }
  })
  t.after(() => closeServer(server))
  server.registerOperation('rider/getInfo', async () => ({ ok: true }), {
    requestSchema: requestSchema('GET', '/v1/riders/{{request.params.id}}')
  })
  assert.deepEqual(server.capabilities()[0].requestSchema, {
    method: 'GET',
    template: '/v1/riders/{{request.params.id}}',
    body: false,
    query: true,
    params: true,
    headers: true
  })

  const { connection, sent } = fakeConnection(server, 'pod-contract')
  await server._enqueue(connection, {
    type: 'call',
    requestId: 'req-invalid-contract',
    operation: 'rider/getInfo',
    payload: { id: 'rider-42' },
    _request: { body: { forbidden: true } }
  })
  assert.equal(enqueued, 0)
  assert.equal(sent[0].error.code, 'REQUEST_VALIDATION_FAILED')
  assert.equal(sent[0]._request.output.statusCode, null)
  assert.match(sent[0]._request.output.error.message, /GET requests must use _request.query/)
})

test('a successful Redis idempotency claim enqueues and returns a queue id', async t => {
  const calls = []
  const server = new GatewayServer({
    queue: {
      claimIdempotency: async () => ({ claimed: true }),
      enqueue: async item => {
        calls.push(item)
        return { id: '1710000000000-0', stream: 'test:queue:normal', priority: 'normal' }
      }
    }
  })
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async () => 'ok')
  const { connection, sent } = fakeConnection(server)
  await server._enqueue(connection, {
    type: 'call',
    requestId: 'request-new',
    operation: 'demo/echo',
    payload: { hello: true },
    idempotencyKey: 'idem-new'
  })
  assert.equal(calls.length, 1)
  assert.equal(calls[0].request.requestId, 'request-new')
  assert.deepEqual(sent, [{ type: 'accepted', requestId: 'request-new', queueId: '1710000000000-0' }])
})

test('duplicate claims return an accepted alias or cached result without enqueueing', async t => {
  let enqueues = 0
  const server = new GatewayServer({
    queue: {
      claimIdempotency: async (_pod, key) => key === 'cached'
        ? { claimed: false, requestId: 'original-cached', response: { type: 'result', requestId: 'original-cached', ok: true, payload: 7 } }
        : { claimed: false, requestId: 'original-inflight' },
      enqueue: async () => { enqueues++; return { id: 'never' } }
    }
  })
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async () => 'ok')
  const first = fakeConnection(server, 'pod-dup')
  await server._enqueue(first.connection, { type: 'call', requestId: 'req-inflight', operation: 'demo/echo', idempotencyKey: 'inflight' })
  assert.deepEqual(first.sent[0], { type: 'accepted', requestId: 'req-inflight', duplicateOf: 'original-inflight' })
  const second = fakeConnection(server, 'pod-cached')
  await server._enqueue(second.connection, { type: 'call', requestId: 'req-cached', operation: 'demo/echo', idempotencyKey: 'cached' })
  assert.deepEqual(second.sent[0], { type: 'result', requestId: 'req-cached', ok: true, payload: 7, duplicateOf: 'original-cached' })
  assert.equal(enqueues, 0)
})

test('validates operation, authentication and malformed messages', async t => {
  const server = new GatewayServer()
  t.after(() => closeServer(server))
  const { connection, sent } = fakeConnection(server)
  connection.authenticated = false
  await server._message(connection, Buffer.from('{bad'))
  await server._message(connection, Buffer.from(JSON.stringify({ type: 'call', requestId: 'req-unauth', operation: 'demo/echo' })))
  assert.equal(sent[0].error.code, 'INVALID_MESSAGE')
  assert.equal(sent[1].error.code, 'NOT_AUTHENTICATED')
  connection.authenticated = true
  await server._message(connection, Buffer.from(JSON.stringify({ type: 'call', requestId: 'req-invalid', operation: 'bad' })))
  assert.equal(sent[2].error.code, 'INVALID_REQUEST')
})

test('completes the hello handshake and advertises registered operations', async t => {
  const server = new GatewayServer({ authenticate: async message => message.token === 'secret' })
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async () => 'ok', { label: 'Echo' })
  const { connection, sent } = fakeConnection(server, 'pod-hello')
  connection.authenticated = false
  await server._hello(connection, { type: 'hello', protocolVersion: '1.0', podId: 'pod-hello', instanceId: 'instance-1', token: 'secret' })
  assert.equal(connection.authenticated, true)
  assert.equal(connection.instanceId, 'instance-1')
  assert.equal(sent[0].type, 'hello_ack')
  assert.deepEqual(sent[0].capabilities, [{ operation: 'demo/echo', label: 'Echo' }])
  await server._hello(connection, { type: 'hello', podId: 'pod-hello', token: 'wrong' })
  assert.equal(sent.length, 1)
})

test('rejects protocol mismatch, missing POD ID and failed authentication', async t => {
  const server = new GatewayServer({ authenticate: async () => false })
  t.after(() => closeServer(server))
  const mismatch = fakeConnection(server, 'pod-mismatch')
  mismatch.connection.authenticated = false
  await server._hello(mismatch.connection, { type: 'hello', protocolVersion: '9.9', podId: 'pod-mismatch' })
  assert.equal(mismatch.sent[0].type, 'hello_error')
  assert.equal(mismatch.sent[0].error.code, 'UNSUPPORTED_PROTOCOL')

  const missing = fakeConnection(server, 'pod-missing')
  missing.connection.authenticated = false
  await server._hello(missing.connection, { type: 'hello', protocolVersion: '1.0' })
  assert.equal(missing.sent[0].error.code, 'AUTH_FAILED')

  const auth = fakeConnection(server, 'pod-auth')
  auth.connection.authenticated = false
  await server._hello(auth.connection, { type: 'hello', protocolVersion: '1.0', podId: 'pod-auth' })
  assert.equal(auth.sent[0].error.code, 'AUTH_FAILED')
})

test('unknown operations and queue failures produce normalized errors', async t => {
  const server = new GatewayServer({
    queue: {
      claimIdempotency: async () => ({ claimed: true }),
      enqueue: async () => { throw Object.assign(new Error('full'), { code: 'QUEUE_FULL' }) },
      releaseIdempotency: async (...args) => { server.released = args }
    }
  })
  t.after(() => closeServer(server))
  const unknown = fakeConnection(server, 'pod-unknown')
  await server._enqueue(unknown.connection, { type: 'call', requestId: 'req-unknown', operation: 'demo/missing', idempotencyKey: 'i-unknown' })
  assert.equal(unknown.sent[0].error.code, 'OPERATION_NOT_FOUND')

  server.registerOperation('demo/echo', async () => 'ok')
  const failed = fakeConnection(server, 'pod-failed')
  await server._enqueue(failed.connection, { type: 'call', requestId: 'req-full', operation: 'demo/echo', idempotencyKey: 'i-full' })
  assert.equal(failed.sent[0].error.code, 'QUEUE_FULL')
  assert.deepEqual(server.released, ['pod-failed', 'i-full', 'req-full'])
})

test('executes an adapter and delivers the result to the originating connection', async t => {
  const server = new GatewayServer()
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async (payload, context) => {
    assert.equal(context.service, 'demo')
    assert.equal(context.operation, 'echo')
    assert.equal(context.attempt, 1)
    return { echoed: payload }
  })
  const { connection, sent } = fakeConnection(server, 'pod-execute')
  await server._execute({
    connectionId: connection.connectionId,
    podId: connection.podId,
    request: { requestId: 'req-execute', operation: 'demo/echo', payload: 3 },
    operation: { service: 'demo', operation: 'echo', value: 'demo/echo' },
    enqueuedAt: Date.now(),
    attempts: 0
  })
  assert.equal(sent[0].type, 'result')
  assert.equal(sent[0].ok, true)
  assert.deepEqual(sent[0].payload, { echoed: 3 })
})

test('passes the normalized request contract to the adapter and returns the upstream status', async t => {
  const server = new GatewayServer()
  t.after(() => closeServer(server))
  server.registerOperation('rider/update', async (payload, context) => {
    assert.deepEqual(payload, { id: 'rider-42' })
    assert.equal(context.method, 'PUT')
    assert.deepEqual(context.request, {
      params: { id: 'rider-42' },
      query: { notify: true },
      body: { active: false }
    })
    context.response = { statusCode: 204 }
    return { updated: true }
  }, { requestSchema: requestSchema('PUT', '/v1/riders/{{request.params.id}}') })
  const { connection, sent } = fakeConnection(server, 'pod-status')
  await server._execute({
    connectionId: connection.connectionId,
    podId: connection.podId,
    request: {
      requestId: 'req-status',
      operation: 'rider/update',
      payload: { id: 'rider-42' },
      _request: { params: { id: 'rider-42' }, query: { notify: true }, body: { active: false } }
    },
    operation: {
      service: 'rider',
      operation: 'update',
      value: 'rider/update',
      requestSchema: requestSchema('PUT', '/v1/riders/{{request.params.id}}')
    },
    receivedAt: Date.now(),
    enqueuedAt: Date.now(),
    attempts: 0
  })
  assert.equal(sent[0].response.statusCode, 204)
  assert.equal(sent[0]._request.input.method, 'PUT')
  assert.deepEqual(sent[0]._request.input.body, { active: false })
  assert.equal(sent[0]._request.output.statusCode, 204)
  assert.deepEqual(sent[0]._request.output.body, { updated: true })
})

test('emits lifecycle and latency metrics for an upstream request', async t => {
  const metrics = []
  const server = new GatewayServer()
  server.on('metric', metric => metrics.push(metric))
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async payload => {
    await new Promise(resolve => setTimeout(resolve, 2))
    return { echoed: payload }
  })
  const { connection, sent } = fakeConnection(server, 'pod-metrics')
  await server._execute({
    connectionId: connection.connectionId,
    podId: connection.podId,
    request: { requestId: 'req-metrics', operation: 'demo/echo', payload: 3 },
    operation: { service: 'demo', operation: 'echo', value: 'demo/echo' },
    receivedAt: Date.now(),
    enqueuedAt: Date.now(),
    lastQueuedAt: Date.now(),
    queueWaitMs: 0,
    rateLimitWaitMs: 0,
    upstreamMs: 0,
    attempts: 0
  })
  const completed = metrics.find(metric => metric.event === 'request.completed')
  assert.ok(metrics.some(metric => metric.event === 'upstream.started'))
  assert.ok(metrics.some(metric => metric.event === 'upstream.completed'))
  assert.equal(completed.service, 'demo')
  assert.equal(completed.operation, 'echo')
  assert.equal(completed.outcome, 'success')
  assert.equal(completed.status, 'completed')
  assert.ok(completed.timings.upstreamMs >= 0)
  assert.ok(completed.timings.totalMs >= completed.timings.upstreamMs)
  assert.equal(typeof completed.timings.queueMs, 'number')
  assert.equal(typeof completed.timings.deliveryMs, 'number')
  assert.equal(sent[0].durationMs, completed.durationMs)
})

test('returns adapter errors and expired queued items to the POD', async t => {
  const server = new GatewayServer({ upstreamTimeoutMs: 20 })
  t.after(() => closeServer(server))
  server.registerOperation('demo/fail', async () => { throw Object.assign(new Error('provider down'), { code: 'UPSTREAM', retryable: false }) })
  const failed = fakeConnection(server, 'pod-failed-adapter')
  await server._execute({
    connectionId: failed.connection.connectionId,
    podId: failed.connection.podId,
    request: { requestId: 'req-failed-adapter', operation: 'demo/fail', payload: null },
    operation: { service: 'demo', operation: 'fail', value: 'demo/fail' },
    enqueuedAt: Date.now(),
    attempts: 0
  })
  assert.equal(failed.sent[0].error.code, 'UPSTREAM')

  server.registerOperation('demo/http-fail', async () => {
    throw Object.assign(new Error('provider rejected the request'), {
      code: 'UPSTREAM_HTTP_422',
      retryable: false,
      status: 422,
      responseBody: { error: 'invalid rider' }
    })
  }, { requestSchema: requestSchema('POST', '/v1/riders') })
  const httpFailed = fakeConnection(server, 'pod-http-failed')
  await server._execute({
    connectionId: httpFailed.connection.connectionId,
    podId: httpFailed.connection.podId,
    request: { requestId: 'req-http-failed', operation: 'demo/http-fail', payload: { id: 'rider-42' }, _request: { body: { id: 'rider-42' } } },
    operation: { service: 'demo', operation: 'http-fail', value: 'demo/http-fail', requestSchema: requestSchema('POST', '/v1/riders') },
    enqueuedAt: Date.now(),
    attempts: 0
  })
  assert.equal(httpFailed.sent[0].response.statusCode, 422)
  assert.equal(httpFailed.sent[0]._request.output.statusCode, 422)
  assert.deepEqual(httpFailed.sent[0]._request.output.body, { error: 'invalid rider' })

  const expired = fakeConnection(server, 'pod-expired')
  await server._handleQueuedItem({
    connectionId: expired.connection.connectionId,
    podId: expired.connection.podId,
    request: { requestId: 'req-expired', operation: 'demo/fail', deadlineAt: new Date(Date.now() - 1).toISOString() },
    operation: { service: 'demo', operation: 'fail', value: 'demo/fail' },
    enqueuedAt: Date.now(),
    attempts: 0
  })
  assert.equal(expired.sent[0].error.code, 'DEADLINE_EXCEEDED')
})

test('returns a cached idempotency result before executing a queued item', async t => {
  let executed = false
  const server = new GatewayServer({
    queue: { getIdempotencyResult: async () => ({ response: { type: 'result', requestId: 'old', ok: true, payload: 'cached' } }) }
  })
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async () => { executed = true; return 'fresh' })
  const { connection, sent } = fakeConnection(server, 'pod-cache')
  await server._handleQueuedItem({
    connectionId: connection.connectionId,
    podId: connection.podId,
    request: { requestId: 'new', operation: 'demo/echo', idempotencyKey: 'same' },
    operation: { service: 'demo', operation: 'echo', value: 'demo/echo' },
    enqueuedAt: Date.now(),
    attempts: 0
  })
  assert.equal(executed, false)
  assert.equal(sent[0].duplicateOf, 'old')
  assert.equal(sent[0].requestId, 'new')
})

test('re-executes a reclaimed item when no idempotency result was stored yet (gateway crashed before finishing)', async t => {
  let executed = 0
  let stored = null
  const server = new GatewayServer({
    queue: {
      // XAUTOCLAIM redelivers the same Stream entry after a crash. If the
      // gateway died between claiming the idempotency key and storing the
      // result, the cached lookup must come back empty -- the reclaimed
      // dispatch has to actually run the adapter again, not treat the
      // still-open claim as "someone else is handling this".
      getIdempotencyResult: async () => null,
      storeIdempotencyResult: async (_podId, _key, response) => { stored = response }
    }
  })
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async payload => { executed++; return `handled:${payload}` })
  const { connection, sent } = fakeConnection(server, 'pod-reclaim')
  await server._handleQueuedItem({
    connectionId: connection.connectionId,
    podId: connection.podId,
    request: { requestId: 'reclaimed-1', operation: 'demo/echo', payload: 'x', idempotencyKey: 'crash-key' },
    operation: { service: 'demo', operation: 'echo', value: 'demo/echo' },
    enqueuedAt: Date.now(),
    attempts: 0,
    dedupKey: 'pod-reclaim:crash-key'
  })
  assert.equal(executed, 1, 'reclaim must re-run the adapter, not silently no-op on the stale in-flight claim')
  assert.equal(sent[0].ok, true)
  assert.equal(sent[0].payload, 'handled:x')
  assert.equal(stored.requestId, 'reclaimed-1', 'the reclaimed run must (re-)store the idempotency result so a fresh duplicate resolves')
})

test('transfers rate-limited Redis work to delayed retry storage without XADD churn', async t => {
  const scheduled = []
  const server = new GatewayServer({
    queue: {
      scheduleRetry: async (...args) => scheduled.push(args)
    },
    rateLimiter: { allow: async () => ({ allowed: false, retryAfterMs: 25 }) }
  })
  t.after(() => closeServer(server))
  server.registerOperation('demo/echo', async () => 'must-not-run')
  const { connection, sent } = fakeConnection(server, 'pod-delayed')
  const outcome = await server._handleQueuedItem({
    connectionId: connection.connectionId,
    podId: connection.podId,
    request: { requestId: 'req-delayed', operation: 'demo/echo' },
    operation: { service: 'demo', operation: 'echo', value: 'demo/echo' },
    enqueuedAt: Date.now(),
    lastQueuedAt: Date.now(),
    attempts: 0
  })
  assert.deepEqual(outcome, { deferred: true })
  assert.equal(scheduled.length, 1)
  assert.equal(scheduled[0][1], 25)
  assert.deepEqual(scheduled[0][2], { preserveDepth: true })
  assert.deepEqual(sent, [])
})

test('uses delayed retry storage for retryable upstream errors', async t => {
  const scheduled = []
  const server = new GatewayServer({
    queue: {
      scheduleRetry: async (...args) => scheduled.push(args)
    },
    maxAttempts: 1
  })
  t.after(() => closeServer(server))
  server.registerOperation('demo/retry', async () => {
    throw Object.assign(new Error('try later'), { code: 'UPSTREAM_HTTP_503', retryable: true, retryAfterMs: 33 })
  })
  const { connection, sent } = fakeConnection(server, 'pod-retry')
  const outcome = await server._handleQueuedItem({
    connectionId: connection.connectionId,
    podId: connection.podId,
    request: { requestId: 'req-retry', operation: 'demo/retry' },
    operation: { service: 'demo', operation: 'retry', value: 'demo/retry' },
    enqueuedAt: Date.now(),
    lastQueuedAt: Date.now(),
    attempts: 0
  })
  assert.deepEqual(outcome, { deferred: true })
  assert.equal(scheduled[0][1], 33)
  assert.deepEqual(scheduled[0][2], { preserveDepth: true })
  assert.deepEqual(sent, [])
})

test('stores a result when the original socket is unavailable', async t => {
  const pending = []
  const server = new GatewayServer({ queue: { storePendingResult: async (_pod, response) => pending.push(response) } })
  t.after(() => closeServer(server))
  await server._deliverResponse({ podId: 'pod-offline', connectionId: 'gone', request: { requestId: 'req-offline' } }, {
    type: 'result', requestId: 'req-offline', ok: true, payload: 'saved'
  })
  assert.deepEqual(pending, [{ type: 'result', requestId: 'req-offline', ok: true, payload: 'saved' }])
})

test('health endpoint reports readiness and queue depth', async t => {
  const server = new GatewayServer()
  t.after(() => closeServer(server))
  const responses = []
  const response = { writeHead: (...args) => responses.push(args), end: body => { responses.push(JSON.parse(body)) } }
  server._health({ url: '/healthz' }, response)
  assert.equal(responses[0][0], 503)
  server.started = true
  server._health({ url: '/readyz' }, response)
  assert.equal(responses[2][0], 200)
  server._health({ url: '/unknown' }, response)
  assert.equal(responses[4][0], 404)
})

test('health endpoint goes unready when the Redis queue connection drops', async t => {
  let queueUp = true
  const server = new GatewayServer({ queue: { isReady: () => queueUp } })
  server.started = true
  t.after(() => closeServer(server))
  const responses = []
  const response = { writeHead: (...args) => responses.push(args), end: body => { responses.push(JSON.parse(body)) } }

  server._health({ url: '/healthz' }, response)
  assert.equal(responses[0][0], 200)
  assert.equal(responses[1].queueReady, true)

  queueUp = false
  server._health({ url: '/healthz' }, response)
  assert.equal(responses[2][0], 503)
  assert.equal(responses[3].queueReady, false)
  assert.equal(responses[3].ok, false)
})

test('warns once per operation when an upstream timeout exceeds the queue claimIdleMs', async t => {
  const warnings = []
  const logger = { debug () {}, info () {}, error () {}, warn: (...args) => warnings.push(args.join(' ')) }
  const server = new GatewayServer({ queue: { claimIdleMs: 5000 }, logger })
  t.after(() => closeServer(server))

  server._checkClaimIdleMargin({ operation: { value: 'demo/slow' } }, 8000)
  server._checkClaimIdleMargin({ operation: { value: 'demo/slow' } }, 9000) // same op again -> no repeat warning
  server._checkClaimIdleMargin({ operation: { value: 'demo/other' } }, 8000) // different op -> warns
  server._checkClaimIdleMargin({ operation: { value: 'demo/fast' } }, 2000) // under claimIdleMs -> no warning

  assert.equal(warnings.length, 2)
  assert.match(warnings[0], /demo\/slow/)
  assert.match(warnings[0], /5000/)
  assert.match(warnings[1], /demo\/other/)
})

test('does not warn about claimIdleMs when the queue does not expose one (in-memory queue)', async t => {
  const warnings = []
  const logger = { debug () {}, info () {}, error () {}, warn: (...args) => warnings.push(args) }
  const server = new GatewayServer({ logger })
  t.after(() => closeServer(server))
  server._checkClaimIdleMargin({ operation: { value: 'demo/x' } }, 999999)
  assert.equal(warnings.length, 0)
})

test('health endpoint goes unready when the Redis rate limiter connection drops', async t => {
  const server = new GatewayServer({ rateLimiter: { client: { isReady: false } } })
  server.started = true
  t.after(() => closeServer(server))
  const responses = []
  const response = { writeHead: (...args) => responses.push(args), end: body => { responses.push(JSON.parse(body)) } }
  server._health({ url: '/healthz' }, response)
  assert.equal(responses[0][0], 503)
  assert.equal(responses[1].rateLimiterReady, false)
})
