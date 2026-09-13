'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { createJsonHttpAdapter, UpstreamError } = require('../lib/http-adapter')

function response (body, options = {}) {
  const headers = new Map(Object.entries(options.headers || { 'content-type': 'application/json' }))
  return {
    ok: options.ok ?? true,
    status: options.status || 200,
    headers: { get: name => headers.get(name.toLowerCase()) || headers.get(name) || '' },
    async json () { return body },
    async text () { return typeof body === 'string' ? body : JSON.stringify(body) }
  }
}

test('JSON adapter sends GET without a request body and maps the response', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response({ time: '12:00' })
  }

  const adapter = createJsonHttpAdapter({
    url: 'https://api.example.test/time',
    method: 'GET',
    headers: { Authorization: 'secret' },
    mapResponse: result => ({ ...result, mapped: true })
  })
  const result = await adapter({ timezone: 'Europe/Kyiv' }, {
    requestId: 'req-get',
    idempotencyKey: 'idem-get',
    signal: new AbortController().signal
  })

  assert.deepEqual(result, { time: '12:00', mapped: true })
  assert.equal(request.url, 'https://api.example.test/time')
  assert.equal(request.options.method, 'GET')
  assert.equal(request.options.body, undefined)
  assert.equal(request.options.headers.Authorization, 'secret')
  assert.equal(request.options.headers.accept, 'application/json')
})

test('JSON adapter serializes POST payload and adds idempotency header', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (_url, options) => {
    request = options
    return response({ ok: true })
  }

  const adapter = createJsonHttpAdapter({
    url: 'https://api.example.test/orders',
    method: 'POST',
    idempotencyHeader: 'Idempotency-Key'
  })
  await adapter({ amount: 42 }, {
    requestId: 'req-post',
    idempotencyKey: 'idem-post',
    signal: new AbortController().signal
  })

  assert.equal(request.headers['content-type'], 'application/json')
  assert.equal(request.headers['Idempotency-Key'], 'idem-post')
  assert.equal(request.body, JSON.stringify({ amount: 42 }))
})

test('JSON adapter applies _request query, headers and body according to the HTTP method', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  const requests = []
  global.fetch = async (url, options) => {
    requests.push({ url, options })
    return response({ updated: true })
  }

  const adapter = createJsonHttpAdapter({
    url: 'https://api.example.test/riders/42?fixed=1',
    method: 'PUT',
    headers: { Authorization: 'gateway-secret' }
  })
  await adapter({ id: 42, ignored: true }, {
    requestId: 'req-contract',
    request: {
      query: { include: ['profile', 'orders'] },
      headers: { 'X-Correlation-ID': 'corr-1', Authorization: 'pod-secret' },
      body: { name: 'Updated rider' }
    }
  })

  assert.equal(requests[0].url, 'https://api.example.test/riders/42?fixed=1&include=profile&include=orders')
  assert.equal(requests[0].options.method, 'PUT')
  assert.equal(requests[0].options.body, JSON.stringify({ name: 'Updated rider' }))
  assert.equal(requests[0].options.headers['X-Correlation-ID'], 'corr-1')
  assert.equal(requests[0].options.headers.Authorization, 'gateway-secret')

  const getAdapter = createJsonHttpAdapter({ url: 'https://api.example.test/riders', method: 'GET' })
  await getAdapter({ id: 42 }, { request: { query: { id: 42 }, body: { mustNotBeSent: true } } })
  assert.equal(requests[1].url, 'https://api.example.test/riders?id=42')
  assert.equal(requests[1].options.body, undefined)
})

test('JSON adapter resolves dynamic request headers without URL encoding', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response({ ok: true })
  }

  const adapter = createJsonHttpAdapter({
    url: 'https://api.example.test/riders',
    method: 'GET',
    headers: { 'X-Static': 'gateway' }
  })
  await adapter({ token: 'token/42' }, {
    requestId: 'req-header',
    request: {
      headers: {
        'X-Trace': '{{gateway.requestId}}',
        'X-Token': 'Bearer {{payload.token}}'
      }
    }
  })

  assert.equal(request.options.headers['X-Trace'], 'req-header')
  assert.equal(request.options.headers['X-Token'], 'Bearer token/42')
  assert.equal(request.options.headers['X-Static'], 'gateway')
  assert.equal(request.options.headers['X-Token'].includes('%2F'), false)
})

test('a POD cannot override a custom-named credential header via _request.headers', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response({ ok: true })
  }

  // Mirrors what nodes/gateway-adapter.js builds when the Gateway API Config
  // uses a non-standard credential header name: the real secret lives in
  // options.headers, and options.protectedHeaders names it so a POD's own
  // _request.headers can never win the spread in http-adapter.js.
  const adapter = createJsonHttpAdapter({
    url: 'https://api.example.test/riders',
    method: 'GET',
    headers: { 'X-My-Secret': 'the-real-api-key' },
    protectedHeaders: ['X-My-Secret']
  })
  await adapter({}, {
    requestId: 'req-header-attack',
    request: { headers: { 'X-My-Secret': 'attacker-supplied-value' } }
  })

  assert.equal(request.options.headers['X-My-Secret'], 'the-real-api-key')
})

test('without protectedHeaders, only the static list of well-known credential headers is defended', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response({ ok: true })
  }

  const adapter = createJsonHttpAdapter({
    url: 'https://api.example.test/riders',
    method: 'GET',
    headers: { authorization: 'Bearer the-real-token' }
  })
  await adapter({}, {
    requestId: 'req-header-static',
    request: { headers: { authorization: 'attacker-supplied-value' } }
  })

  assert.equal(request.options.headers.authorization, 'Bearer the-real-token')
})

test('JSON adapter handles a HEAD response without parsing a body', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let options
  global.fetch = async (_url, value) => {
    options = value
    return response({ should: 'not be parsed' }, { status: 204 })
  }
  const result = await createJsonHttpAdapter({ url: 'https://api.example.test/health', method: 'HEAD' })({}, {})
  assert.equal(result, null)
  assert.equal(options.method, 'HEAD')
  assert.equal(options.body, undefined)
})

test('JSON adapter exposes the upstream status and response body on HTTP errors', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  global.fetch = async () => response({ error: 'invalid rider' }, {
    ok: false,
    status: 422,
    headers: { 'content-type': 'application/json' }
  })
  const context = { request: {} }
  await assert.rejects(
    createJsonHttpAdapter({ url: 'https://api.example.test/riders', method: 'POST' })({}, context),
    error => error.code === 'UPSTREAM_HTTP_422' && error.status === 422 && error.responseBody.error === 'invalid rider' && context.response.statusCode === 422
  )
})

test('JSON adapter supports async headers, URL and body builders', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response('accepted', { headers: { 'content-type': 'text/plain' } })
  }

  const adapter = createJsonHttpAdapter({
    method: 'PUT',
    buildUrl: async payload => `https://api.example.test/items/${payload.id}`,
    headers: async (_payload, context) => ({ 'X-Request-ID': context.requestId }),
    buildBody: async payload => ({ value: payload.value })
  })
  const result = await adapter({ id: 'a/1', value: 7 }, {
    requestId: 'req-build',
    signal: new AbortController().signal
  })

  assert.equal(result, 'accepted')
  assert.equal(request.url, 'https://api.example.test/items/a/1')
  assert.equal(request.options.headers['X-Request-ID'], 'req-build')
  assert.equal(request.options.body, JSON.stringify({ value: 7 }))
})

test('JSON adapter normalizes retryable upstream HTTP errors and Retry-After', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  global.fetch = async () => response({ error: 'slow down' }, {
    ok: false,
    status: 429,
    headers: { 'content-type': 'application/json', 'retry-after': '2' }
  })

  const adapter = createJsonHttpAdapter({ url: 'https://api.example.test/rate' })
  await assert.rejects(
    adapter({}, { requestId: 'req-429', signal: new AbortController().signal }),
    error => error instanceof UpstreamError && error.code === 'UPSTREAM_HTTP_429' && error.retryable && error.retryAfterMs === 2000 && error.status === 429
  )
})

test('JSON adapter maps non-retryable HTTP and network errors', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  global.fetch = async () => response('bad request', {
    ok: false,
    status: 400,
    headers: { 'content-type': 'text/plain' }
  })

  const adapter = createJsonHttpAdapter({ url: 'https://api.example.test/bad' })
  await assert.rejects(
    adapter({}, { requestId: 'req-400', signal: new AbortController().signal }),
    error => error.code === 'UPSTREAM_HTTP_400' && !error.retryable && error.status === 400
  )

  global.fetch = async () => { throw new Error('DNS failure') }
  await assert.rejects(
    adapter({}, { requestId: 'req-network', signal: new AbortController().signal }),
    error => error.code === 'UPSTREAM_NETWORK_ERROR' && error.retryable
  )
})

test('JSON adapter turns an aborted fetch into a retryable timeout', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  global.fetch = async () => {
    const error = new Error('aborted')
    error.name = 'AbortError'
    throw error
  }

  const adapter = createJsonHttpAdapter({ url: 'https://api.example.test/timeout' })
  await assert.rejects(
    adapter({}, { requestId: 'req-timeout', signal: new AbortController().signal }),
    error => error.code === 'UPSTREAM_TIMEOUT' && error.retryable
  )
})

test('JSON adapter validates that either url or buildUrl is provided', () => {
  assert.throws(() => createJsonHttpAdapter(), /url or buildUrl is required/)
})

test('JSON adapter keeps connections alive by default and does not force Connection: close', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response({ ok: true })
  }

  const adapter = createJsonHttpAdapter({ url: 'https://api.example.test/keepalive', method: 'GET' })
  await adapter({}, {})

  assert.equal(request.options.headers.connection, undefined)
})

test('JSON adapter sends Connection: close on every request when keepAlive is disabled', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response({ ok: true })
  }

  const adapter = createJsonHttpAdapter({ url: 'https://api.example.test/keepalive', method: 'GET', keepAlive: false })
  await adapter({}, {})

  assert.equal(request.options.headers.connection, 'close')
})

test('JSON adapter keepAlive:false cannot be overridden back on by a POD-supplied _request.headers', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let request
  global.fetch = async (url, options) => {
    request = { url, options }
    return response({ ok: true })
  }

  const adapter = createJsonHttpAdapter({ url: 'https://api.example.test/keepalive', method: 'GET', keepAlive: false })
  await adapter({}, { request: { headers: { connection: 'keep-alive' } } })

  assert.equal(request.options.headers.connection, 'close')
})
