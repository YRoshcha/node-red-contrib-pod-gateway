'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { randomUUID } = require('node:crypto')
const { RedisGcraRateLimiter } = require('../lib/redis-rate-limiter')
const { RedisStreamQueue } = require('../lib/redis-stream-queue')
const { GatewayServer } = require('../lib/gateway-server')
const { GatewayClient } = require('../lib/gateway-client')

function sleep (delayMs) {
  return new Promise(resolve => setTimeout(resolve, delayMs))
}

// bin/pod-gateway-load.js already exercises a real Gateway + Redis under load
// as a manual, unbounded script (npm run test:load) that only prints a
// report -- nobody runs it in CI and it asserts nothing. This is the same
// real-stack pattern (real GatewayServer, real RedisStreamQueue, real
// WebSocket client), scaled down to a bounded burst with actual assertions,
// so backpressure behavior is checked on every `npm test` run that has
// Redis available, and skips cleanly (like the other real-Redis tests) when
// it does not.
test('a concurrent burst well over maxQueueSize is rejected with QUEUE_FULL, not dropped, hung or double-counted', async t => {
  const prefix = `test-backpressure-${randomUUID()}`
  const url = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
  const maxQueueSize = 3
  let limiter
  let queue
  let server
  let client
  try {
    limiter = await RedisGcraRateLimiter.connect({ url, keyPrefix: `${prefix}-rl`, limits: {} })
    queue = await RedisStreamQueue.connect({ url, prefix, maxQueueSize, blockMs: 10, retryPollMs: 10 })
  } catch (error) {
    t.skip(`Redis is not available: ${error.message}`)
    return
  }

  // Slow enough that the whole burst is sent well before even one request
  // can finish and free its depth slot -- without that gap this is a race
  // that sometimes drains fast enough to never actually overflow.
  server = new GatewayServer({
    port: 0,
    queue,
    rateLimiter: limiter,
    concurrency: 4,
    adapters: {
      'load/echo': async payload => { await sleep(150); return { ok: true, index: payload.index } }
    }
  })
  await server.start()
  client = new GatewayClient({
    url: `ws://127.0.0.1:${server.httpServer.address().port}/ws`,
    podId: `${prefix}-pod`,
    reconnectInterval: 25
  })
  t.after(async () => {
    client.close()
    await server.stop()
  })

  const burstSize = 20
  const settled = await Promise.allSettled(
    Array.from({ length: burstSize }, (_, index) => client.request({
      type: 'call',
      operation: 'load/echo',
      payload: { index },
      deadlineAt: new Date(Date.now() + 10000).toISOString()
    }, 10000))
  )

  const fulfilled = settled.filter(result => result.status === 'fulfilled')
  const rejected = settled.filter(result => result.status === 'rejected')

  assert.ok(fulfilled.length > 0, 'requests within capacity must still complete normally')
  assert.ok(
    rejected.length > 0,
    `a burst of ${burstSize} against maxQueueSize=${maxQueueSize} must overflow -- got 0 rejections, the burst may have drained faster than expected`
  )
  for (const result of rejected) {
    assert.equal(result.reason.code, 'QUEUE_FULL')
    assert.equal(result.reason.retryable, true)
  }

  // A rejected QUEUE_FULL attempt must never have counted against depth in
  // the first place, and every accepted item must clear its depth slot once
  // it completes -- either way, the queue drains back to exactly 0, not to
  // some leftover count from the overflowed attempts.
  const deadline = Date.now() + 3000
  let depth = await queue.depth()
  while (depth !== 0 && Date.now() < deadline) {
    await sleep(25)
    depth = await queue.depth()
  }
  assert.equal(depth, 0)
})
