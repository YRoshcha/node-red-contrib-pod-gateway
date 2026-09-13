'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { parseHeaders, expandEnvironment, joinUrl, requestOptions } = require('../lib/api-config')

test('joins an API base URL and path', () => {
  assert.equal(joinUrl('https://api.example.com/', '/v1/send'), 'https://api.example.com/v1/send')
})

test('builds reusable API request headers', () => {
  const result = requestOptions({
    baseUrl: 'https://api.example.com',
    headers: parseHeaders('{"Accept":"application/json"}'),
    apiKeyHeader: 'Authorization',
    apiKeyPrefix: 'App ',
    apiKey: 'secret'
  }, '/send', { 'X-Trace': 'demo' })
  assert.equal(result.url, 'https://api.example.com/send')
  assert.deepEqual(result.headers, {
    Accept: 'application/json',
    Authorization: 'App secret',
    'X-Trace': 'demo'
  })
})

test('parses and recursively expands headers without mutating input', () => {
  const previous = process.env.POD_GATEWAY_TEST_HEADER
  process.env.POD_GATEWAY_TEST_HEADER = 'expanded'
  try {
    const source = { nested: { value: '${POD_GATEWAY_TEST_HEADER}' }, list: ['${POD_GATEWAY_TEST_HEADER}'] }
    assert.deepEqual(expandEnvironment(source), { nested: { value: 'expanded' }, list: ['expanded'] })
    assert.deepEqual(source, { nested: { value: '${POD_GATEWAY_TEST_HEADER}' }, list: ['${POD_GATEWAY_TEST_HEADER}'] })
    assert.deepEqual(parseHeaders('{"X-Test":"${POD_GATEWAY_TEST_HEADER}"}'), { 'X-Test': 'expanded' })
  } finally {
    if (previous === undefined) delete process.env.POD_GATEWAY_TEST_HEADER
    else process.env.POD_GATEWAY_TEST_HEADER = previous
  }
})

test('rejects invalid header values and API URLs', () => {
  assert.throws(() => parseHeaders('[]'), /Headers must be a JSON object/)
  assert.throws(() => parseHeaders('{bad'), /Expected property|Unexpected token/)
  assert.throws(() => joinUrl('', '/path'), /API base URL is required/)
  assert.throws(() => joinUrl('not a URL', '/path'), /Invalid API base URL/)
  assert.equal(joinUrl('https://api.example.test/', ''), 'https://api.example.test/')
})

test('lets per-request headers override reusable headers and omits an empty API key', () => {
  const result = requestOptions({
    baseUrl: 'https://api.example.test',
    headers: { Accept: 'application/json', 'X-Mode': 'default' },
    apiKeyHeader: 'X-Api-Key',
    apiKey: ''
  }, '/ping', { 'X-Mode': 'override' })
  assert.deepEqual(result.headers, { Accept: 'application/json', 'X-Mode': 'override' })
})
