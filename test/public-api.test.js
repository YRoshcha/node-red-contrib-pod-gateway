'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')

test('exports the documented embedding API', () => {
  const api = require('..')
  for (const name of [
    'GatewayClient', 'GatewayServer', 'RedisGcraRateLimiter', 'RedisStreamQueue',
    'InMemoryGcraRateLimiter', 'createJsonHttpAdapter', 'UpstreamError',
    'loadDotEnv', 'loadGatewayConfig', 'normalizeRedisUrl', 'createLogger',
    'apiConfig', 'protocol', 'requestContract'
  ]) assert.equal(typeof api[name], 'object' === typeof api[name] ? 'object' : 'function')
  assert.equal(typeof api.apiConfig.parseHeaders, 'function')
  assert.equal(typeof api.protocol.validateEnvelope, 'function')
  assert.equal(typeof api.requestContract.validateRequest, 'function')
})
