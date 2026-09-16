'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { EventEmitter } = require('node:events')

function makeRED (nodes = {}) {
  const registered = {}
  const statuses = []
  const errors = []
  const sent = []
  const RED = {
    nodes: {
      createNode: (node, config) => {
        EventEmitter.call(node)
        Object.setPrototypeOf(node, EventEmitter.prototype)
        node.id = config.id || 'test-node'
        node.credentials = config.credentials || {}
        node.status = value => statuses.push({ id: node.id, value })
        node.error = value => errors.push({ id: node.id, value })
        node.send = message => sent.push(message)
        // Tests that need {{global.x}} header resolution pass a plain
        // `globalContext` object in the node config; everything else gets
        // an always-empty global context, matching a real deploy with no
        // global context configured.
        const globalStore = config.globalContext || {}
        node.context = () => ({ global: { get: name => globalStore[name] } })
      },
      registerType: (name, constructor) => { registered[name] = constructor },
      getNode: id => nodes[id]
    },
    util: { cloneMessage: message => JSON.parse(JSON.stringify(message)) },
    log: { info: () => {}, warn: () => {}, error: () => {} },
    httpAdmin: null,
    statuses,
    errors,
    sent,
    registered
  }
  return RED
}

class FakeClient extends EventEmitter {
  constructor () {
    super()
    this.state = 'connected'
    this.requests = []
    this.events = []
    this.nextResult = { payload: { ok: true }, durationMs: 12 }
    this.nextError = null
  }

  async connect () { this.connected = true }

  async request (request) {
    this.requests.push(request)
    this.emit('accepted', { type: 'accepted', requestId: request.requestId, queueId: '1-0' })
    if (this.nextError) throw this.nextError
    return this.nextResult
  }

  async send (request) {
    this.events.push(request)
    this.emit('accepted', { type: 'accepted', requestId: request.requestId, queueId: '2-0' })
    return { type: 'accepted', requestId: request.requestId, queueId: '2-0' }
  }

  close () { this.closed = true }
}

test('Gateway Call node sends a successful result and preserves message metadata', async () => {
  const client = new FakeClient()
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-call')(RED)
  const Node = RED.registered['pod-gateway-call']
  const node = new Node({ id: 'call', gateway: 'gateway', operation: 'demo/echo', timeout: 1000, priority: 'normal' })
  const outputs = []
  let done = 0
  node.emit('input', { payload: { value: 1 }, topic: 'demo' }, (messages) => outputs.push(messages), () => { done++ })
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(client.requests[0].operation, 'demo/echo')
  assert.deepEqual(outputs[0][0].payload, { ok: true })
  assert.equal(outputs[0][1], null)
  assert.equal(outputs[0][0].gateway.status, 'completed')
  assert.equal(done, 1)
})

test('Gateway Call node sends normalized errors to output 2', async () => {
  const client = new FakeClient()
  client.nextError = Object.assign(new Error('upstream failed'), {
    code: 'UPSTREAM_HTTP_500',
    retryable: true,
    statusCode: 500,
    responseBody: { error: 'provider down' }
  })
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-call')(RED)
  const node = new RED.registered['pod-gateway-call']({ id: 'call-error', gateway: 'gateway', operation: 'demo/echo', timeout: 1000 })
  const outputs = []
  node.emit('input', { payload: 'x' }, messages => outputs.push(messages), () => {})
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(outputs[0][0], null)
  assert.equal(outputs[0][1].error.code, 'UPSTREAM_HTTP_500')
  assert.equal(outputs[0][1].gateway.status, 'failed')
  assert.equal(outputs[0][1]._request.output.statusCode, 500)
  assert.deepEqual(outputs[0][1]._request.output.body, { error: 'provider down' })
})

test('Gateway Call validates the API request contract and exposes input/output status metadata', async () => {
  const client = new FakeClient()
  client.capabilities = [{
    operation: 'rider/update',
    requestSchema: { method: 'PUT', template: '/v1/riders/{{request.params.id}}', body: true, query: true, params: true, headers: true }
  }]
  client.nextResult = { payload: { updated: true }, response: { statusCode: 200 }, durationMs: 8 }
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-call')(RED)
  const node = new RED.registered['pod-gateway-call']({ id: 'call-contract', gateway: 'gateway', operation: 'rider/update', timeout: 1000 })
  const outputs = []
  node.emit('input', {
    payload: { id: 'rider-42', tenantId: 'tenant/42' },
    _request: {
      params: { id: 'rider-42' },
      query: { notify: true },
      headers: { 'X-Tenant-ID': '{{payload.tenantId}}' },
      body: { active: false }
    }
  }, messages => outputs.push(messages), () => {})
  await new Promise(resolve => setImmediate(resolve))

  assert.equal(outputs[0][1], null)
  assert.equal(outputs[0][0]._request.input.method, 'PUT')
  assert.deepEqual(outputs[0][0]._request.input.body, { active: false })
  assert.deepEqual(client.requests[0]._request.headers, { 'X-Tenant-ID': '{{payload.tenantId}}' })
  assert.equal(outputs[0][0]._request.output.statusCode, 200)
  assert.equal(outputs[0][0].gateway.httpStatus, 200)
})

test('Gateway Call rejects a body supplied to a GET operation before sending it', async () => {
  const client = new FakeClient()
  client.capabilities = [{
    operation: 'rider/getInfo',
    requestSchema: { method: 'GET', template: '/v1/riders/{{request.params.id}}', body: false, query: true, params: true, headers: true }
  }]
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-call')(RED)
  const node = new RED.registered['pod-gateway-call']({ id: 'call-invalid-contract', gateway: 'gateway', operation: 'rider/getInfo', timeout: 1000 })
  const outputs = []
  node.emit('input', {
    payload: { id: 'rider-42' },
    _request: { body: { forbidden: true } }
  }, messages => outputs.push(messages), () => {})
  await new Promise(resolve => setImmediate(resolve))

  assert.equal(client.requests.length, 0)
  assert.equal(outputs[0][0], null)
  assert.equal(outputs[0][1].error.code, 'REQUEST_VALIDATION_FAILED')
  assert.match(outputs[0][1].error.message, /GET requests must use _request.query/)
  assert.ok(outputs[0][1].error.details.some(detail => /GET requests/.test(detail)))
  assert.equal(outputs[0][1]._request.output.statusCode, null)
})

test('Gateway Out node reports accepted work without waiting for its result', async () => {
  const client = new FakeClient()
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-out')(RED)
  const node = new RED.registered['pod-gateway-out']({ id: 'out', gateway: 'gateway', operation: 'demo/echo', priority: 'bulk', acceptTimeout: 1000 })
  const outputs = []
  node.emit('input', { payload: { async: true } }, messages => outputs.push(messages), () => {})
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(client.events[0].type, 'event')
  assert.equal(client.events[0].priority, 'bulk')
  assert.equal(outputs[0][0].gateway.status, 'accepted')
  assert.equal(outputs[0][1], null)
})

test('Gateway Out sends the same method-aware request contract', async () => {
  const client = new FakeClient()
  client.capabilities = [{
    operation: 'rider/update',
    requestSchema: { method: 'PATCH', template: '/v1/riders/{{request.params.id}}', body: true, query: true, params: true, headers: true }
  }]
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-out')(RED)
  const node = new RED.registered['pod-gateway-out']({ id: 'out-contract', gateway: 'gateway', operation: 'rider/update', priority: 'bulk', acceptTimeout: 1000 })
  const outputs = []
  node.emit('input', {
    payload: { id: 'rider-42' },
    _request: { params: { id: 'rider-42' }, body: { active: false } }
  }, messages => outputs.push(messages), () => {})
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(client.events[0]._request.body.active, false)
  assert.equal(outputs[0][0]._request.input.method, 'PATCH')
  assert.equal(outputs[0][0]._request.output.status, 'accepted')
})

test('Gateway In node filters operations and emits gateway metadata', async () => {
  const client = new FakeClient()
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-in')(RED)
  const node = new RED.registered['pod-gateway-in']({ id: 'in', gateway: 'gateway', event: 'demo/echo' })
  client.emit('message', { type: 'result', requestId: 'req-1', operation: 'demo/missing', payload: 1 })
  client.emit('message', { type: 'result', requestId: 'req-2', operation: 'demo/echo', payload: { ok: true } })
  assert.equal(RED.sent.length, 1)
  assert.deepEqual(RED.sent[0].payload, { ok: true })
  assert.equal(RED.sent[0].gateway.operationKey, 'demo/echo')
  assert.equal(RED.sent[0].gateway.status, 'received')
  node.emit('close', false, () => {})
  client.emit('message', { type: 'result', requestId: 'req-3', operation: 'demo/echo', payload: 3 })
  assert.equal(RED.sent.length, 1)
})

test('Gateway In forwards the canonical request envelope and HTTP status', async () => {
  const client = new FakeClient()
  const RED = makeRED({ gateway: { client } })
  require('../nodes/gateway-in')(RED)
  const node = new RED.registered['pod-gateway-in']({ id: 'in-contract', gateway: 'gateway', event: 'rider/getInfo' })
  const requestEnvelope = {
    input: { method: 'GET', params: { id: 'rider-42' }, payload: { id: 'rider-42' } },
    output: { statusCode: 200, body: { id: 'rider-42' } }
  }
  client.emit('message', {
    type: 'result',
    requestId: 'req-in-contract',
    operation: 'rider/getInfo',
    payload: { id: 'rider-42' },
    response: { statusCode: 200 },
    _request: requestEnvelope
  })
  assert.deepEqual(RED.sent[0]._request, requestEnvelope)
  assert.equal(RED.sent[0].gateway.httpStatus, 200)
})

test('Gateway Metrics node forwards filtered gateway events for downstream metrics palettes', () => {
  const { EventEmitter } = require('node:events')
  const server = new EventEmitter()
  const RED = makeRED({ server })
  require('../nodes/gateway-metrics')(RED)
  const node = new RED.registered['pod-gateway-metrics']({
    id: 'metrics',
    server: 'server',
    events: 'request.completed, upstream.completed'
  })
  const completed = {
    event: 'request.completed',
    timestamp: '2026-08-31T10:00:00.000Z',
    requestId: 'req-1',
    podId: 'pod-1',
    service: 'worldtime',
    operation: 'getKyivTime',
    operationKey: 'worldtime/getKyivTime',
    outcome: 'success',
    timings: { queueMs: 5, rateLimitMs: 0, upstreamMs: 42, deliveryMs: 1, totalMs: 50 }
  }
  server.emit('gateway-metric', completed)
  server.emit('gateway-metric', { ...completed, event: 'ignored.event' })
  assert.equal(RED.sent.length, 1)
  assert.equal(RED.sent[0].topic, 'pod-gateway/request.completed')
  assert.deepEqual(RED.sent[0].payload.timings, completed.timings)
  assert.equal(RED.sent[0].metric, completed)
  node.emit('close', false, () => {})
  server.emit('gateway-metric', completed)
  assert.equal(RED.sent.length, 1)
})

test('Gateway API Config validates settings and builds credentialed request options', () => {
  const RED = makeRED()
  require('../nodes/gateway-api-config')(RED)
  const node = new RED.registered['pod-gateway-api-config']({
    id: 'api',
    baseUrl: 'https://api.example.test',
    headers: '{"Accept":"application/json"}',
    apiKeyHeader: 'X-Api-Key',
    credentials: { apiKey: 'secret' }
  })
  assert.equal(node.configError, null)
  assert.deepEqual(node.buildRequestOptions('/v1/ping', { 'X-Trace': '1' }), {
    url: 'https://api.example.test/v1/ping',
    headers: { Accept: 'application/json', 'X-Api-Key': 'secret', 'X-Trace': '1' }
  })
  assert.equal(RED.errors.length, 0)
})

test('Gateway Adapter registers a provider operation using API config headers', async () => {
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  const api = {
    buildRequestOptions: (path, headers) => ({ url: `https://api.example.test${path}`, headers: { ...headers, Authorization: 'secret' } })
  }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter', server: 'server', api: 'api', operation: 'demo/echo', path: '/echo', method: 'POST', headers: '{}', rate: '60', rateUnit: 'minute', burst: '60'
  })
  await node.ready
  assert.equal(registrations.length, 1)
  assert.equal(registrations[0][0], 'demo/echo')
  assert.equal(registrations[0][2].label, 'demo/echo')
  assert.deepEqual(registrations[0][2].rateLimit, { rate: 1, burst: 60 })
  assert.deepEqual(registrations[0][2].requestSchema, {
    method: 'POST',
    template: '/echo',
    body: true,
    query: true,
    params: true,
    headers: true
  })
  node.emit('close', false, () => {})
})

test('Gateway Adapter resolves payload variables in an API path per request', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let requestUrl
  global.fetch = async url => {
    requestUrl = url
    return {
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      async json () { return { ok: true } },
      async text () { return '{"ok":true}' }
    }
  }
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  const api = {
    buildRequestOptions: (path, headers) => ({ url: `https://api.example.test${path}`, headers: { ...headers } })
  }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter-template', server: 'server', api: 'api', operation: 'rider/getInfo', path: '/v1/riders/{{payload.id}}', method: 'GET', headers: '{}'
  })
  await node.ready
  const result = await registrations[0][1]({ id: 'rider/42' }, {
    requestId: 'req-template',
    signal: new AbortController().signal
  })
  assert.deepEqual(result, { ok: true })
  assert.equal(requestUrl, 'https://api.example.test/v1/riders/rider%2F42')
  node.emit('close', false, () => {})
})

test('Gateway Adapter resolves {{global.x}} in API config and adapter headers per request', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let sentHeaders
  global.fetch = async (url, options) => {
    sentHeaders = options.headers
    return {
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      async json () { return { ok: true } },
      async text () { return '{"ok":true}' }
    }
  }
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  // Mirrors the real Gateway API Config merge: its own headers first, then
  // the adapter's own headers layered on top -- both may carry templates.
  const api = {
    buildRequestOptions: (path, headers) => ({
      url: `https://api.example.test${path}`,
      headers: { 'X-Tenant': '{{global.tenantId}}', ...headers }
    })
  }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter-global-header',
    server: 'server',
    api: 'api',
    operation: 'demo/withGlobalAuth',
    path: '/echo',
    method: 'POST',
    headers: '{"Authorization":"Bearer {{global.apiToken}}"}',
    globalContext: { apiToken: 'g-secret-token', tenantId: 'tenant-9' }
  })
  await node.ready
  await registrations[0][1]({ ok: true }, { requestId: 'req-global', signal: new AbortController().signal })
  assert.equal(sentHeaders.Authorization, 'Bearer g-secret-token')
  assert.equal(sentHeaders['X-Tenant'], 'tenant-9')
  node.emit('close', false, () => {})
})

test('Gateway Adapter surfaces a missing {{global.x}} header variable as a non-retryable gateway error', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  global.fetch = async () => { throw new Error('fetch should not be called') }
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  const api = { buildRequestOptions: (path, headers) => ({ url: `https://api.example.test${path}`, headers: { ...headers } }) }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter-missing-global',
    server: 'server',
    api: 'api',
    operation: 'demo/missingAuth',
    path: '/echo',
    method: 'POST',
    headers: '{"Authorization":"Bearer {{global.apiToken}}"}'
    // no globalContext configured -> global.apiToken is undefined
  })
  await node.ready
  await assert.rejects(
    registrations[0][1]({ ok: true }, { requestId: 'req-missing', signal: new AbortController().signal }),
    error => error.code === 'INVALID_HEADER_TEMPLATE' && error.retryable === false
  )
  node.emit('close', false, () => {})
})

test('Gateway Adapter leaves headers static (no per-request resolver) when none contain a template', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let sentHeaders
  global.fetch = async (url, options) => {
    sentHeaders = options.headers
    return {
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      async json () { return { ok: true } },
      async text () { return '{"ok":true}' }
    }
  }
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  const api = { buildRequestOptions: (path, headers) => ({ url: `https://api.example.test${path}`, headers: { ...headers, 'X-Static': '1' } }) }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter-static-headers', server: 'server', api: 'api', operation: 'demo/static', path: '/echo', method: 'POST', headers: '{}'
  })
  await node.ready
  await registrations[0][1]({ ok: true }, { requestId: 'req-static', signal: new AbortController().signal })
  assert.equal(sentHeaders['X-Static'], '1')
  node.emit('close', false, () => {})
})

test('Header priority: _request overrides Adapter, Adapter overrides API Config', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let sentHeaders
  global.fetch = async (url, options) => {
    sentHeaders = options.headers
    return {
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      async json () { return { ok: true } },
      async text () { return '{"ok":true}' }
    }
  }
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  // API Config sets X-Source and X-Api-Only; Adapter headers are merged in
  // on top of that (mirrors lib/api-config.js#requestOptions), so X-Source
  // is already 'adapter' by the time createJsonHttpAdapter sees it -- this
  // matches how nodes/gateway-api-config.js + nodes/gateway-adapter.js
  // actually merge in production, not a simplified stand-in.
  const api = {
    buildRequestOptions: (path, headers) => ({
      url: `https://api.example.test${path}`,
      headers: { 'X-Source': 'api-config', 'X-Api-Only': 'api', ...headers }
    })
  }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter-header-priority',
    server: 'server',
    api: 'api',
    operation: 'demo/headerPriority',
    path: '/echo',
    method: 'POST',
    // Adapter's own header: overrides API Config's X-Source, adds its own.
    headers: '{"X-Source":"adapter","X-Adapter-Only":"adapter"}'
  })
  await node.ready
  // A POD's _request.headers is merged last, in lib/http-adapter.js, after
  // the config-level headers this test's registrations[0][1] already
  // carries baked in -- so it overrides both config layers.
  await registrations[0][1](
    { ok: true },
    {
      requestId: 'req-priority',
      signal: new AbortController().signal,
      request: { headers: { 'X-Source': 'pod', 'X-Pod-Only': 'pod' } }
    }
  )
  assert.equal(sentHeaders['X-Source'], 'pod') // _request wins over both config levels
  assert.equal(sentHeaders['X-Api-Only'], 'api') // untouched by adapter or _request
  assert.equal(sentHeaders['X-Adapter-Only'], 'adapter') // untouched by _request, but set by adapter over API Config's absence
  assert.equal(sentHeaders['X-Pod-Only'], 'pod') // only _request sets this one
  node.emit('close', false, () => {})
})

test('Header priority holds with {{global.x}} templates: _request still wins, template still resolves at the layer below it', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let sentHeaders
  global.fetch = async (url, options) => {
    sentHeaders = options.headers
    return {
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      async json () { return { ok: true } },
      async text () { return '{"ok":true}' }
    }
  }
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  const api = {
    buildRequestOptions: (path, headers) => ({
      url: `https://api.example.test${path}`,
      headers: { 'X-Session': 'Bearer {{global.apiToken}}', ...headers }
    })
  }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter-priority-with-global',
    server: 'server',
    api: 'api',
    operation: 'demo/priorityWithGlobal',
    path: '/echo',
    method: 'POST',
    headers: '{}',
    globalContext: { apiToken: 'g-secret' }
  })
  await node.ready
  // Case 1: POD sends no X-Session -> the API Config's {{global.x}} value resolves through.
  await registrations[0][1]({ ok: true }, { requestId: 'req-1', signal: new AbortController().signal })
  assert.equal(sentHeaders['X-Session'], 'Bearer g-secret')
  // Case 2: a POD's own X-Session still wins. Note this would NOT hold for
  // Authorization, Cookie, Host or the other names on request-contract.js's
  // static PROTECTED_HEADERS list -- those are unconditionally controlled
  // by config (adapter/API Config), never overridable from a POD, by
  // design, regardless of whether an apiKeyHeader is configured. The
  // _request > adapter > API Config priority this test checks applies to
  // ordinary, non-credential header names.
  await registrations[0][1](
    { ok: true },
    { requestId: 'req-2', signal: new AbortController().signal, request: { headers: { 'X-Session': 'Bearer pod-token' } } }
  )
  assert.equal(sentHeaders['X-Session'], 'Bearer pod-token')
  node.emit('close', false, () => {})
})

test('Authorization from {{global.apiToken}} at API Config level: resolves per request, and a POD cannot override it', async t => {
  const originalFetch = global.fetch
  t.after(() => { global.fetch = originalFetch })
  let sentHeaders
  global.fetch = async (url, options) => {
    sentHeaders = options.headers
    return {
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      async json () { return { ok: true } },
      async text () { return '{"ok":true}' }
    }
  }
  const registrations = []
  const server = {
    logger: { debug: () => {}, error: () => {} },
    registerOperation: async (...args) => registrations.push(args),
    unregisterOperation: async () => {}
  }
  // This is the motivating case for {{global.x}}: a token refreshed by
  // another flow into global context, forwarded upstream as Authorization
  // from the API Config -- exactly the header a POD's own _request.headers
  // could never carry, since Authorization is unconditionally on
  // request-contract.js's static PROTECTED_HEADERS list.
  const api = {
    buildRequestOptions: (path, headers) => ({
      url: `https://api.example.test${path}`,
      headers: { Authorization: 'Bearer {{global.apiToken}}', ...headers }
    })
  }
  const RED = makeRED({ server, api })
  require('../nodes/gateway-adapter')(RED)
  const globalContext = { apiToken: 'first-token' }
  const node = new RED.registered['pod-gateway-adapter']({
    id: 'adapter-auth-from-global',
    server: 'server',
    api: 'api',
    operation: 'demo/authFromGlobal',
    path: '/echo',
    method: 'POST',
    headers: '{}',
    globalContext
  })
  await node.ready

  // Resolves fresh from global context on every call -- not baked in once
  // at deploy -- so a token refreshed mid-flow by another part of the flow
  // (e.g. an OAuth refresh timer writing to the same global context key)
  // takes effect on the very next request with no redeploy.
  await registrations[0][1]({ ok: true }, { requestId: 'req-auth-1', signal: new AbortController().signal })
  assert.equal(sentHeaders.Authorization, 'Bearer first-token')

  globalContext.apiToken = 'refreshed-token'
  await registrations[0][1]({ ok: true }, { requestId: 'req-auth-2', signal: new AbortController().signal })
  assert.equal(sentHeaders.Authorization, 'Bearer refreshed-token')

  // A POD trying to send its own Authorization is silently dropped by
  // requestHeaders() (lib/request-contract.js), regardless of priority --
  // the API Config's resolved value stands.
  await registrations[0][1](
    { ok: true },
    { requestId: 'req-auth-3', signal: new AbortController().signal, request: { headers: { Authorization: 'Bearer pod-supplied' } } }
  )
  assert.equal(sentHeaders.Authorization, 'Bearer refreshed-token')

  node.emit('close', false, () => {})
})

test('Gateway Adapter rejects an invalid operation rate limit', () => {
  const server = { logger: { debug: () => {}, error: () => {} }, registerOperation: async () => {}, unregisterOperation: async () => {} }
  const RED = makeRED({ server })
  require('../nodes/gateway-adapter')(RED)
  new RED.registered['pod-gateway-adapter']({
    id: 'adapter-invalid-rate', server: 'server', operation: 'demo/echo', url: 'https://api.example.test/echo', rate: '0', rateUnit: 'second', burst: '1'
  })
  assert.match(RED.errors[0].value, /Rate limit must be a positive number/)
})

test('Gateway Adapter rejects a burst smaller than its rate', () => {
  const server = { logger: { debug: () => {}, error: () => {} }, registerOperation: async () => {}, unregisterOperation: async () => {} }
  const RED = makeRED({ server })
  require('../nodes/gateway-adapter')(RED)
  new RED.registered['pod-gateway-adapter']({
    id: 'adapter-small-burst', server: 'server', operation: 'demo/echo', url: 'https://api.example.test/echo', rate: '20', rateUnit: 'second', burst: '1'
  })
  assert.match(RED.errors[0].value, /Burst must be greater than or equal to rate/)
})

test('Gateway Adapter rejects the non-standard UPDATE method without throwing from the node constructor', () => {
  const server = { logger: { debug: () => {}, error: () => {} }, registerOperation: async () => {}, unregisterOperation: async () => {} }
  const RED = makeRED({ server })
  require('../nodes/gateway-adapter')(RED)
  assert.doesNotThrow(() => new RED.registered['pod-gateway-adapter']({
    id: 'adapter-update-method',
    server: 'server',
    operation: 'demo/update',
    url: 'https://api.example.test/items',
    method: 'UPDATE'
  }))
  assert.match(RED.errors[0].value, /Unsupported HTTP method UPDATE/)
})
