'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { RedisGcraRateLimiter, GCRA_SCRIPT } = require('../lib/redis-rate-limiter')

test('bypasses Redis when a service has no configured limit', async () => {
  let evaluated = false
  const limiter = new RedisGcraRateLimiter({ client: { isReady: true, eval: async () => { evaluated = true } }, limits: {} })
  assert.deepEqual(await limiter.allow('unconfigured'), { allowed: true, retryAfterMs: 0 })
  assert.equal(evaluated, false)
})

test('runs the atomic GCRA script with a service key', async () => {
  let invocation
  const limiter = new RedisGcraRateLimiter({
    client: {
      isReady: true,
      eval: async (...args) => { invocation = args; return [1, 0] }
    },
    keyPrefix: 'limits',
    limits: { worldtime: { rate: 5, burst: 2 } }
  })
  assert.deepEqual(await limiter.allow('worldtime', 2), { allowed: true, retryAfterMs: 0 })
  assert.equal(invocation[1].keys[0], 'limits:{worldtime}:g')
  assert.deepEqual(invocation[1].arguments, ['1', '5', '2', '2'])
  assert.match(invocation[0], /redis\.call\('TIME'\)/)
  assert.ok(GCRA_SCRIPT.length > 100)
})

test('prefers an operation-specific bucket and falls back to the service bucket', async () => {
  const invocations = []
  const limiter = new RedisGcraRateLimiter({
    client: {
      isReady: true,
      eval: async (...args) => { invocations.push(args); return [1, 0] }
    },
    keyPrefix: 'limits',
    limits: {
      rider: { rate: 100, burst: 2 },
      'rider/getInfo': { rate: 20, burst: 1 },
      'rider/update': { rate: 1, burst: 1 }
    }
  })

  await limiter.allow('rider', 1, 'rider/getInfo')
  await limiter.allow('rider', 1, 'rider/update')
  await limiter.allow('rider', 1, 'rider/list')

  assert.equal(invocations[0][1].keys[0], 'limits:{rider/getInfo}:g')
  assert.deepEqual(invocations[0][1].arguments, ['1', '20', '1', '1'])
  assert.equal(invocations[1][1].keys[0], 'limits:{rider/update}:g')
  assert.deepEqual(invocations[1][1].arguments, ['1', '1', '1', '1'])
  assert.equal(invocations[2][1].keys[0], 'limits:{rider}:g')
  assert.deepEqual(invocations[2][1].arguments, ['1', '100', '2', '1'])
})

test('returns a retry delay when Redis denies the request', async () => {
  const limiter = new RedisGcraRateLimiter({
    client: { isReady: true, eval: async () => [0, 125] },
    limits: { demo: { rate: 10, burst: 1 } }
  })
  assert.deepEqual(await limiter.allow('demo'), { allowed: false, retryAfterMs: 125 })
})

test('reports Redis unavailability and closes the client', async () => {
  let quit = 0
  const client = { isReady: false, isOpen: true, quit: async () => { quit++ } }
  const limiter = new RedisGcraRateLimiter({ client, limits: { demo: { rate: 1, burst: 1 } } })
  await assert.rejects(limiter.allow('demo'), error => error.code === 'RATE_LIMITER_UNAVAILABLE')
  await limiter.close()
  assert.equal(quit, 1)
})
