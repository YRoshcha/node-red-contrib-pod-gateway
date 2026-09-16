'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { EventEmitter } = require('node:events')
const { operationFromNode, gatewayMeta, errorMessage, setConnectionStatus, closeListener, globalContextAccessor } = require('../lib/node-utils')

test('derives operations from node config or message metadata', () => {
  assert.deepEqual(operationFromNode({ operation: 'demo/echo' }, {}), { service: 'demo', operation: 'echo', value: 'demo/echo' })
  assert.equal(operationFromNode({ operation: '' }, { gateway: { operation: 'demo/run' } }).value, 'demo/run')
  assert.throws(() => operationFromNode({ operation: '' }, {}), /non-empty string/)
})

test('creates stable gateway metadata and normalized error messages', () => {
  const meta = gatewayMeta('req-1', { service: 'demo', operation: 'echo', value: 'demo/echo' }, 'completed', { extra: true })
  assert.deepEqual(meta, {
    requestId: 'req-1', service: 'demo', operation: 'echo', operationKey: 'demo/echo', status: 'completed', extra: true
  })
  const RED = { util: { cloneMessage: message => ({ ...message }) } }
  const output = errorMessage(RED, { payload: 1 }, Object.assign(new Error('bad'), { code: 'UPSTREAM', retryable: true, retryAfterMs: 20 }), meta)
  assert.equal(output.payload, 1)
  assert.deepEqual(output.error, { code: 'UPSTREAM', message: 'bad', retryable: true, retryAfterMs: 20 })
  assert.equal(output.gateway.status, 'failed')
})

test('maps client state to Node-RED status', () => {
  const states = []
  const node = { status: value => states.push(value) }
  for (const state of ['connecting', 'connected', 'reconnecting', 'disconnected', 'closed', 'unknown']) setConnectionStatus(node, state)
  assert.deepEqual(states.map(value => value.text), ['connecting', 'connected', 'reconnecting', 'disconnected', 'closed', 'disconnected'])
})

test('globalContextAccessor proxies top-level reads to global.get and indexes nested values normally', () => {
  const store = { apiToken: 'secret-123', tenant: { id: 't-1' } }
  const node = { context: () => ({ global: { get: name => store[name] } }) }
  const accessor = globalContextAccessor(node)
  assert.equal(accessor.apiToken, 'secret-123')
  assert.equal(accessor.tenant.id, 't-1')
  assert.equal(accessor.missing, undefined)
})

test('globalContextAccessor returns an empty, always-undefined view when the node has no context', () => {
  assert.equal(globalContextAccessor(null).anything, undefined)
  assert.equal(globalContextAccessor({}).anything, undefined)
})

test('closeListener removes listeners and calls Node-RED done callback', () => {
  const node = new EventEmitter()
  const client = new EventEmitter()
  let calls = 0
  const listener = () => { calls++ }
  client.on('state', listener)
  closeListener(node, client, 'state', listener)
  node.emit('close', false, () => { calls += 10 })
  client.emit('state', 'connected')
  assert.equal(calls, 10)
})
