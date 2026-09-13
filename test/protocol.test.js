'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { parseOperation, validateEnvelope, errorPayload, makeRequestId, PRIORITY, PROTOCOL_VERSION } = require('../lib/protocol')

test('parseOperation splits a service operation', () => {
  assert.deepEqual(parseOperation('catalog/getItem'), {
    service: 'catalog',
    operation: 'getItem',
    value: 'catalog/getItem'
  })
})

test('parseOperation rejects an incomplete operation', () => {
  assert.throws(() => parseOperation('sendSms'), /service\/name/)
})

test('validateEnvelope validates call and event envelopes', () => {
  assert.equal(validateEnvelope({
    type: 'call',
    requestId: 'req-1',
    operation: 'crm/createLead'
  }).type, 'call')
  assert.throws(() => validateEnvelope({ type: 'call', operation: 'crm/createLead' }), /requestId/)
})

test('supports operation names with documented characters and rejects unsafe values', () => {
  assert.equal(parseOperation('service.v2/send:bulk-name').service, 'service.v2')
  assert.throws(() => parseOperation('service name/run'), /unsupported characters/)
  assert.throws(() => parseOperation('service/run value'), /unsupported characters/)
  assert.throws(() => parseOperation('/run'), /service\/name/)
  assert.throws(() => parseOperation('service/'), /service\/name/)
})

test('validates generic messages and normalizes error fields', () => {
  assert.equal(validateEnvelope({ type: 'heartbeat' }).type, 'heartbeat')
  assert.throws(() => validateEnvelope(null), /message must be an object/)
  assert.throws(() => validateEnvelope({}), /message.type is required/)
  const payload = errorPayload(Object.assign(new Error('retry'), { code: 'BUSY', retryable: true, retryAfterMs: '15' }))
  assert.deepEqual(payload, { code: 'BUSY', message: 'retry', retryable: true, retryAfterMs: 15 })
  assert.deepEqual(errorPayload(new Error()), { code: 'GATEWAY_ERROR', message: 'Gateway request failed', retryable: false })
})

test('exports protocol constants and creates unique request ids', () => {
  assert.equal(PROTOCOL_VERSION, '1.0')
  assert.equal(PRIORITY.normal, 1)
  assert.equal(PRIORITY.bulk, 0)
  assert.notEqual(makeRequestId(), makeRequestId())
})
