'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')
const { loadGatewayConfig } = require('../lib/runtime-config')

test('loads Redis and gateway settings from an explicit JSON config', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'pod-gateway-config-'))
  const file = path.join(directory, 'gateway.config.json')
  try {
    fs.writeFileSync(file, JSON.stringify({
      redis: { url: 'redis://redis.internal:6379', keyPrefix: 'orders', maxQueueSize: 42, resultTtlSeconds: 17, deleteOnAck: false, retryPollMs: 50 },
      server: { port: 9090, concurrency: 7 },
      auth: { token: 'secret' },
      rateLimits: { upstream: { rate: 12, burst: 1 } }
    }))

    const config = loadGatewayConfig(file)
    assert.equal(config.redis.url, 'redis://redis.internal:6379')
    assert.equal(config.redis.keyPrefix, 'orders')
    assert.equal(config.redis.maxQueueSize, 42)
    assert.equal(config.redis.resultTtlSeconds, 17)
    assert.equal(config.redis.deleteOnAck, false)
    assert.equal(config.redis.retryPollMs, 50)
    assert.equal(config.server.port, 9090)
    assert.equal(config.server.concurrency, 7)
    assert.equal(config.auth.token, 'secret')
    assert.deepEqual(config.rateLimits, { upstream: { rate: 12, burst: 1 } })
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
})

test('applies deployment environment overrides over file settings', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'pod-gateway-config-env-'))
  const file = path.join(directory, 'gateway.config.json')
  const names = ['REDIS_URL', 'REDIS_KEY_PREFIX', 'GATEWAY_MAX_QUEUE', 'PORT', 'GATEWAY_CONCURRENCY', 'GATEWAY_TOKEN', 'GATEWAY_RATE_LIMITS', 'REDIS_RESULT_TTL_SECONDS', 'REDIS_DELETE_ON_ACK', 'REDIS_RETRY_POLL_MS']
  const previous = Object.fromEntries(names.map(name => [name, process.env[name]]))
  fs.writeFileSync(file, JSON.stringify({
    redis: { url: 'redis://file:6379', keyPrefix: 'file', maxQueueSize: 1 },
    server: { port: 1, concurrency: 1 },
    auth: { token: 'file-token' },
    rateLimits: { file: { rate: 1, burst: 1 } }
  }))
  Object.assign(process.env, {
    REDIS_URL: 'rediss://env:6380',
    REDIS_KEY_PREFIX: 'env',
    GATEWAY_MAX_QUEUE: '99',
    PORT: '9080',
    GATEWAY_CONCURRENCY: '9',
    GATEWAY_TOKEN: 'env-token',
    GATEWAY_RATE_LIMITS: '{"env":{"rate":20,"burst":2}}',
    REDIS_RESULT_TTL_SECONDS: '99',
    REDIS_DELETE_ON_ACK: 'false',
    REDIS_RETRY_POLL_MS: '75'
  })
  try {
    const config = loadGatewayConfig(file)
    assert.equal(config.redis.url, 'rediss://env:6380')
    assert.equal(config.redis.keyPrefix, 'env')
    assert.equal(config.redis.maxQueueSize, 99)
    assert.equal(config.redis.resultTtlSeconds, 99)
    assert.equal(config.redis.deleteOnAck, false)
    assert.equal(config.redis.retryPollMs, 75)
    assert.equal(config.server.port, 9080)
    assert.equal(config.server.concurrency, 9)
    assert.equal(config.auth.token, 'env-token')
    assert.deepEqual(config.rateLimits, { env: { rate: 20, burst: 2 } })
  } finally {
    for (const name of names) {
      if (previous[name] === undefined) delete process.env[name]
      else process.env[name] = previous[name]
    }
    fs.rmSync(directory, { recursive: true, force: true })
  }
})

test('rejects invalid config JSON, numbers and rate limit JSON', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'pod-gateway-config-invalid-'))
  const invalidJson = path.join(directory, 'invalid.json')
  const invalidNumber = path.join(directory, 'invalid-number.json')
  const invalidLimit = path.join(directory, 'invalid-limit.json')
  fs.writeFileSync(invalidJson, '{bad')
  fs.writeFileSync(invalidNumber, JSON.stringify({ server: { port: 'nope' } }))
  fs.writeFileSync(invalidLimit, JSON.stringify({ rateLimits: 'not-json' }))
  try {
    assert.throws(() => loadGatewayConfig(invalidJson), /Invalid gateway config/)
    assert.throws(() => loadGatewayConfig(invalidNumber), /Expected a number/)
    assert.throws(() => loadGatewayConfig(invalidLimit), /Invalid rateLimits/)
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
})
