'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { GatewayClient } = require('../lib/gateway-client')

function connectedClient () {
  const client = new GatewayClient({ url: 'ws://unused', heartbeatInterval: 100000 })
  const sent = []
  client.connect = async () => {}
  client.ws = { readyState: 1, send: value => sent.push(JSON.parse(value)) }
  client.state = 'connected'
  return { client, sent }
}

function tick (ms = 0) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

test('request resolves after accepted and result messages', async t => {
  const { client, sent } = connectedClient()
  t.after(() => client.close())
  const promise = client.request({ type: 'call', operation: 'demo/echo', payload: 'hello', requestId: 'req-1' }, 500)
  await tick()
  assert.equal(sent[0].requestId, 'req-1')
  client._handleMessage(JSON.stringify({ type: 'accepted', requestId: 'req-1', queueId: '1-0' }))
  client._handleMessage(JSON.stringify({ type: 'result', requestId: 'req-1', operation: 'demo/echo', ok: true, payload: 'world' }))
  assert.deepEqual(await promise, { type: 'result', requestId: 'req-1', operation: 'demo/echo', ok: true, payload: 'world' })
})

test('send resolves on accepted and leaves the later result as a message event', async t => {
  const { client, sent } = connectedClient()
  t.after(() => client.close())
  const messages = []
  client.on('message', message => messages.push(message))
  const promise = client.send({ type: 'event', operation: 'demo/echo', payload: 'async', requestId: 'req-out' }, 500)
  await tick()
  assert.equal(sent[0].type, 'event')
  client._handleMessage(JSON.stringify({ type: 'accepted', requestId: 'req-out', queueId: '2-0' }))
  assert.deepEqual(await promise, { type: 'accepted', requestId: 'req-out', queueId: '2-0' })
  client._handleMessage(JSON.stringify({ type: 'result', requestId: 'req-out', operation: 'demo/echo', ok: true, payload: 'done' }))
  assert.equal(messages.length, 1)
  assert.equal(messages[0].payload, 'done')
})

test('request rejects normalized gateway errors', async t => {
  const { client } = connectedClient()
  t.after(() => client.close())
  const promise = client.request({ type: 'call', operation: 'demo/fail', requestId: 'req-fail' }, 500)
  await tick()
  client._handleMessage(JSON.stringify({
    type: 'result',
    requestId: 'req-fail',
    ok: false,
    error: { code: 'UPSTREAM_HTTP_503', message: 'unavailable', retryable: true, retryAfterMs: 25 }
  }))
  await assert.rejects(promise, error => error.code === 'UPSTREAM_HTTP_503' && error.retryable && error.retryAfterMs === 25)
})

test('propagates upstream status and response body from a failed gateway result', async t => {
  const { client } = connectedClient()
  t.after(() => client.close())
  const promise = client.request({ type: 'call', operation: 'demo/fail', requestId: 'req-http-error' }, 500)
  await tick()
  client._handleMessage(JSON.stringify({
    type: 'result',
    requestId: 'req-http-error',
    ok: false,
    response: { statusCode: 422 },
    _request: { output: { statusCode: 422, body: { error: 'invalid rider' } } },
    error: { code: 'UPSTREAM_HTTP_422', message: 'invalid', retryable: false }
  }))
  await assert.rejects(promise, error => error.statusCode === 422 && error.responseBody.error === 'invalid rider')
})

test('duplicate accepted calls alias the original result', async t => {
  const { client } = connectedClient()
  t.after(() => client.close())
  const original = client.request({ type: 'call', operation: 'demo/echo', requestId: 'req-original' }, 500)
  await tick()
  client._handleMessage(JSON.stringify({ type: 'accepted', requestId: 'req-original', queueId: '1-0' }))
  const duplicate = client.request({ type: 'call', operation: 'demo/echo', requestId: 'req-duplicate' }, 500)
  await tick()
  client._handleMessage(JSON.stringify({ type: 'accepted', requestId: 'req-duplicate', duplicateOf: 'req-original' }))
  client._handleMessage(JSON.stringify({ type: 'result', requestId: 'req-original', ok: true, payload: 42 }))
  assert.equal((await original).payload, 42)
  const duplicateResult = await duplicate
  assert.equal(duplicateResult.payload, 42)
  assert.equal(duplicateResult.requestId, 'req-duplicate')
  assert.equal(duplicateResult.duplicateOf, 'req-original')
})

test('request distinguishes accept timeout from result timeout', async t => {
  const { client } = connectedClient()
  t.after(() => client.close())
  await assert.rejects(
    client.request({ type: 'call', operation: 'demo/echo', requestId: 'req-accept-timeout' }, 100, { acceptTimeoutMs: 10 }),
    error => error.code === 'GATEWAY_ACCEPT_TIMEOUT' && error.retryable
  )

  const pending = client.request({ type: 'call', operation: 'demo/echo', requestId: 'req-result-timeout' }, 30)
  await tick()
  client._handleMessage(JSON.stringify({ type: 'accepted', requestId: 'req-result-timeout' }))
  await assert.rejects(pending, error => error.code === 'GATEWAY_RESULT_TIMEOUT' && error.retryable)
})

test('invalid gateway messages emit an error and unknown results are forwarded', async t => {
  const { client } = connectedClient()
  t.after(() => client.close())
  const errors = []
  const messages = []
  client.on('error', error => errors.push(error))
  client.on('message', message => messages.push(message))
  client._handleMessage('{not-json')
  client._handleMessage(JSON.stringify({ type: 'result', requestId: 'unknown', ok: true, payload: 1 }))
  await tick()
  assert.equal(errors[0].code, 'INVALID_GATEWAY_MESSAGE')
  assert.equal(messages[0].requestId, 'unknown')
})

test('close rejects pending calls and changes state', async () => {
  const { client } = connectedClient()
  const promise = client.request({ type: 'call', operation: 'demo/echo', requestId: 'req-close' }, 1000)
  await tick()
  client.close()
  await assert.rejects(promise, error => error.code === 'GATEWAY_CLOSED')
  assert.equal(client.state, 'closed')
})

test('reconnect delay is jittered around reconnectInterval instead of fixed', () => {
  const client = new GatewayClient({ url: 'ws://unused', reconnectInterval: 1000 })
  const samples = Array.from({ length: 200 }, () => client._reconnectDelay())
  for (const delay of samples) {
    assert.ok(delay >= 700 && delay <= 1300, `delay ${delay} must stay within +/-30% of reconnectInterval`)
  }
  // A synchronized reconnect storm (many PODs disconnecting together) is
  // exactly what jitter must break up -- assert the samples are not all
  // identical, i.e. it is not secretly back to a fixed delay.
  assert.ok(new Set(samples).size > 1, 'reconnect delay must vary between calls')
})

test('_scheduleReconnect uses the jittered delay, not a fixed timer', async () => {
  const client = new GatewayClient({ url: 'ws://unused', reconnectInterval: 1000 })
  const delays = []
  const originalSetTimeout = global.setTimeout
  global.setTimeout = (fn, ms) => { delays.push(ms); return originalSetTimeout(() => {}, 0) }
  try {
    client.connect = async () => {}
    client._scheduleReconnect()
  } finally {
    global.setTimeout = originalSetTimeout
  }
  assert.equal(delays.length, 1)
  assert.ok(delays[0] >= 700 && delays[0] <= 1300)
  clearTimeout(client.reconnectTimer)
})
