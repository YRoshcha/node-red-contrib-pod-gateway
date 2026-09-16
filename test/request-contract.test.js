'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const {
  appendQuery,
  HTTP_METHODS,
  methodUsesBody,
  normalizeRequest,
  requestEnvelope,
  requestHeaders,
  requestSchema,
  validateRequest,
  validationError
} = require('../lib/request-contract')

test('normalizes the canonical msg._request input and accepts a result envelope as input', () => {
  const request = {
    params: { id: 'rider-42' },
    query: { include: ['profile', 'orders'] },
    headers: { 'X-Trace': 'trace-1' },
    body: { enabled: true },
    method: 'PUT'
  }
  assert.deepEqual(normalizeRequest(request), {
    params: { id: 'rider-42' },
    query: { include: ['profile', 'orders'] },
    headers: { 'X-Trace': 'trace-1' },
    body: { enabled: true }
  })
  assert.deepEqual(normalizeRequest(requestEnvelope({ id: 'rider-42' }, request, 'PUT', {
    statusCode: 200,
    body: { ok: true }
  })), normalizeRequest(request))
})

test('validates method-specific request fields and dynamic URL variables', () => {
  const getSchema = requestSchema('GET', '/v1/riders/{{request.params.id}}')
  assert.equal(methodUsesBody('GET'), false)
  assert.equal(methodUsesBody('PATCH'), true)
  assert.equal(HTTP_METHODS.has('DELETE'), true)

  const valid = validateRequest({}, { params: { id: 'rider/42' }, query: { verbose: true } }, getSchema)
  assert.equal(valid.ok, true)

  const bodyOnGet = validateRequest({}, { body: { verbose: true } }, getSchema)
  assert.equal(bodyOnGet.ok, false)
  assert.match(bodyOnGet.errors.join('; '), /GET requests must use _request.query/)

  const missingParam = validateRequest({}, { query: {} }, getSchema)
  assert.equal(missingParam.ok, false)
  assert.match(missingParam.errors.join('; '), /URL template variable "request.params.id" is missing/)

  const wrongMethod = validateRequest({}, { method: 'POST' }, getSchema)
  assert.equal(wrongMethod.ok, false)
  assert.match(wrongMethod.errors.join('; '), /_request.method must be GET/)
})

test('validates dynamic per-request headers against payload and gateway context', () => {
  const schema = requestSchema('GET', '/v1/riders')
  const valid = validateRequest(
    { token: 'token/42' },
    { headers: { Authorization: 'ignored', 'X-Trace': '{{gateway.requestId}}', 'X-Token': 'Bearer {{payload.token}}' } },
    schema,
    { requestId: 'req-42' }
  )
  assert.equal(valid.ok, false)
  assert.match(valid.errors.join('; '), /Authorization is managed by the Gateway API Config/)

  const dynamic = validateRequest(
    { token: 'token/42' },
    { headers: { 'X-Trace': '{{gateway.requestId}}', 'X-Token': 'Bearer {{payload.token}}' } },
    schema,
    { requestId: 'req-42' }
  )
  assert.equal(dynamic.ok, true)

  const missing = validateRequest(
    {},
    { headers: { 'X-Token': 'Bearer {{payload.token}}' } },
    schema
  )
  assert.equal(missing.ok, false)
  assert.match(missing.errors.join('; '), /Header template variable "payload.token" is missing/)

  const injected = validateRequest(
    { value: 'safe\r\nX-Injected: yes' },
    { headers: { 'X-Value': '{{payload.value}}' } },
    schema
  )
  assert.equal(injected.ok, false)
  assert.match(injected.errors.join('; '), /contains an invalid line break/)

  assert.deepEqual(
    requestHeaders({ 'X-Trace': '{{gateway.requestId}}', 'X-Optional': undefined }, {}, { requestId: 'req-42' }),
    { 'X-Trace': 'req-42' }
  )
})

test('rejects a custom-named credential header when the schema declares it protected', () => {
  const schema = requestSchema('GET', '/v1/items')
  schema.protectedHeaders = ['X-My-Secret']

  const attack = validateRequest({}, { headers: { 'X-My-Secret': 'attacker-value' } }, schema)
  assert.equal(attack.ok, false)
  assert.match(attack.errors.join('; '), /_request\.headers\.X-My-Secret is managed by the Gateway API Config/)

  // Still allowed: a header not on the protected list, and the built-in
  // static list continues to work regardless of schema.protectedHeaders.
  const allowed = validateRequest({}, { headers: { 'X-Correlation-Id': 'abc' } }, schema)
  assert.equal(allowed.ok, true)
  const stillStatic = validateRequest({}, { headers: { Authorization: 'attacker-value' } }, schema)
  assert.equal(stillStatic.ok, false)

  assert.deepEqual(
    requestHeaders({ 'X-My-Secret': 'attacker-value', 'X-Ok': 'yes' }, {}, {}, ['X-My-Secret']),
    { 'X-Ok': 'yes' }
  )
})

test('accepts _request built entirely inside a separate vm context/realm (Node-RED Function node)', () => {
  // A Node-RED Function node runs user code in its own vm context, so a
  // literal `{}` created there has a different Object.prototype reference
  // than the main process running this validator -- even though it is
  // otherwise a perfectly ordinary plain object. This is not only about
  // the top-level _request: params, query, headers and body are each
  // isPlainObject-checked individually (see lib/request-contract.js), so
  // all four needed the fix, not just the wrapper object. Build every
  // level of _request in the sandbox to cover all of them at once.
  const vm = require('node:vm')
  const sandbox = vm.createContext({})
  const crossRealmRequest = vm.runInContext(`({
    params: { id: 'rider-42' },
    query: { include: 'profile' },
    headers: { 'X-Correlation-ID': 'corr-1' },
    body: { active: false }
  })`, sandbox)

  assert.notEqual(
    Object.getPrototypeOf(crossRealmRequest),
    Object.prototype,
    'sanity check: the sandbox object must actually be a different realm, or this test proves nothing'
  )

  const schema = requestSchema('PUT', '/v1/riders/{{request.params.id}}')
  const result = validateRequest({ id: 'rider-42', active: false }, crossRealmRequest, schema)
  assert.equal(result.ok, true)
  assert.deepEqual(result.errors, [])
  assert.deepEqual(JSON.parse(JSON.stringify(result.request)), {
    params: { id: 'rider-42' },
    query: { include: 'profile' },
    headers: { 'X-Correlation-ID': 'corr-1' },
    body: { active: false }
  })
})

test('rejects malformed and unsafe per-request overrides', () => {
  const schema = requestSchema('POST', '/v1/items')
  for (const value of [null, [], 'invalid']) {
    const result = validateRequest({}, value, schema)
    assert.equal(result.ok, false)
    assert.match(result.errors[0], /_request must be an object/)
  }

  const invalid = validateRequest({}, {
    url: 'https://attacker.example',
    headers: { Authorization: 'override', 'X-Api-Key': 'override', Nested: { value: true } },
    query: { filter: { value: true } }
  }, schema)
  assert.equal(invalid.ok, false)
  assert.match(invalid.errors.join('; '), /_request.url is not supported/)
  assert.match(invalid.errors.join('; '), /Authorization is managed by the Gateway API Config/)
  assert.match(invalid.errors.join('; '), /X-Api-Key is managed by the Gateway API Config/)
  assert.match(invalid.errors.join('; '), /Nested must be a scalar value/)
  assert.match(invalid.errors.join('; '), /filter must contain scalar values/)

  const error = validationError(invalid)
  assert.equal(error.code, 'REQUEST_VALIDATION_FAILED')
  assert.equal(error.retryable, false)
  assert.equal(error.details.length, invalid.errors.length)
})

test('appends scalar and repeated array query values without overwriting configured query', () => {
  assert.equal(
    appendQuery('https://api.example.test/items?fixed=1', { page: 2, tag: ['a', 'b'], ignored: { nested: true } }),
    'https://api.example.test/items?fixed=1&page=2&tag=a&tag=b'
  )
  assert.equal(appendQuery('/items', { q: 'a b' }), '/items?q=a%20b')
})

test('request envelopes expose method, input payload and upstream status/output', () => {
  assert.deepEqual(requestEnvelope(
    { id: 42 },
    { params: { id: 42 }, body: { name: 'Rider' } },
    'PUT',
    { statusCode: 200, body: { updated: true } }
  ), {
    input: {
      method: 'PUT',
      params: { id: 42 },
      body: { name: 'Rider' },
      payload: { id: 42 }
    },
    output: {
      statusCode: 200,
      body: { updated: true }
    }
  })
})

test('rejects the non-standard UPDATE method with an actionable message', () => {
  assert.throws(() => requestSchema('UPDATE', '/items'), /use GET, POST, PUT, PATCH, DELETE or HEAD/)
})
