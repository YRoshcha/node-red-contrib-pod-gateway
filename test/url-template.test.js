'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { hasUrlTemplate, resolveHeaderTemplate, resolveUrlTemplate } = require('../lib/url-template')

test('resolves nested payload values and URL-encodes them', () => {
  assert.equal(hasUrlTemplate('/v1/riders/{{payload.id}}'), true)
  assert.equal(resolveUrlTemplate('/v1/riders/{{payload.id}}?region={{msg.payload.region}}', {
    id: 'rider/42',
    region: 'Europe/Kyiv'
  }), '/v1/riders/rider%2F42?region=Europe%2FKyiv')
  assert.equal(resolveUrlTemplate('/v1/tags/{{payload.tags[0]}}', { tags: ['primary'] }), '/v1/tags/primary')
})

test('resolves gateway request context values', () => {
  assert.equal(resolveUrlTemplate('/requests/{{gateway.requestId}}', {}, { requestId: 'req 1' }), '/requests/req%201')
})

test('resolves header templates without URL encoding', () => {
  assert.equal(
    resolveHeaderTemplate('Bearer {{payload.token}} / {{gateway.requestId}}', { token: 'token/42' }, { requestId: 'req 1' }),
    'Bearer token/42 / req 1'
  )
})

test('rejects missing or unsafe dynamic header values', () => {
  assert.throws(
    () => resolveHeaderTemplate('Bearer {{payload.token}}', {}, {}),
    error => error.code === 'INVALID_HEADER_TEMPLATE' && /Header template variable/.test(error.message)
  )
  assert.throws(
    () => resolveHeaderTemplate('{{payload.token}}', { token: 'Bearer good\r\nX-Injected: yes' }, {}),
    error => error.code === 'INVALID_HEADER_TEMPLATE' && /line break/.test(error.message)
  )
})

test('rejects missing, non-scalar and unsafe template values', () => {
  assert.throws(
    () => resolveUrlTemplate('/riders/{{payload.id}}', {}, {}),
    error => error.code === 'INVALID_URL_TEMPLATE' && /missing/.test(error.message)
  )
  assert.throws(
    () => resolveUrlTemplate('/riders/{{payload}}', { id: 1 }, {}),
    error => error.code === 'INVALID_URL_TEMPLATE' && /scalar/.test(error.message)
  )
  assert.throws(
    () => resolveUrlTemplate('/riders/{{payload.__proto__}}', {}, {}),
    error => error.code === 'INVALID_URL_TEMPLATE' && /not allowed/.test(error.message)
  )
})

test('leaves URLs without templates unchanged', () => {
  assert.equal(hasUrlTemplate('/v1/riders'), false)
  assert.equal(resolveUrlTemplate('/v1/riders', { id: 42 }), '/v1/riders')
})
