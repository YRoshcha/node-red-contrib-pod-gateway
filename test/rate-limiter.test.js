'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { InMemoryGcraRateLimiter } = require('../lib/in-memory-rate-limiter')

test('in-memory GCRA shares one budget per service', async () => {
  const limiter = new InMemoryGcraRateLimiter({ demo: { rate: 10, burst: 1 } })
  assert.equal((await limiter.allow('demo')).allowed, true)
  const second = await limiter.allow('demo')
  assert.equal(second.allowed, false)
  assert.ok(second.retryAfterMs > 0)
  assert.equal((await limiter.allow('other')).allowed, true)
})

test('uses the default bucket and accounts for request cost', async () => {
  const limiter = new InMemoryGcraRateLimiter({ default: { rate: 10, burst: 2 } })
  assert.equal((await limiter.allow('one', 2)).allowed, true)
  const denied = await limiter.allow('one')
  assert.equal(denied.allowed, false)
  assert.ok(denied.retryAfterMs > 0)
})

test('uses independent operation buckets under the same service', async () => {
  const limiter = new InMemoryGcraRateLimiter({
    rider: { rate: 100, burst: 1 },
    'rider/getInfo': { rate: 20, burst: 1 },
    'rider/update': { rate: 1, burst: 1 }
  })

  assert.equal((await limiter.allow('rider', 1, 'rider/getInfo')).allowed, true)
  assert.equal((await limiter.allow('rider', 1, 'rider/getInfo')).allowed, false)
  assert.equal((await limiter.allow('rider', 1, 'rider/update')).allowed, true)
  assert.equal((await limiter.allow('rider', 1, 'rider/update')).allowed, false)
  assert.equal((await limiter.allow('rider', 1, 'rider/list')).allowed, true)
})

test('bypasses malformed or disabled limits safely', async () => {
  const limiter = new InMemoryGcraRateLimiter({ zero: { rate: 0 }, negative: { rate: -1 } })
  assert.deepEqual(await limiter.allow('zero'), { allowed: true, retryAfterMs: 0 })
  assert.deepEqual(await limiter.allow('negative'), { allowed: true, retryAfterMs: 0 })
})
