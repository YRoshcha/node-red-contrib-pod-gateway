'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { randomUUID } = require('node:crypto')
const { RedisGcraRateLimiter } = require('../lib/redis-rate-limiter')
const { RedisStreamQueue } = require('../lib/redis-stream-queue')
const { createReconnectStrategy } = require('../lib/redis-reconnect')
const { GatewayServer } = require('../lib/gateway-server')
const { GatewayClient } = require('../lib/gateway-client')
const { createClient } = require('redis')

test('real Redis Streams queue processes a POD call', async t => {
  const prefix = `test-${randomUUID()}`
  let limiter
  let queue
  let server
  let client
  try {
    limiter = await RedisGcraRateLimiter.connect({
      url: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
      keyPrefix: `${prefix}-rl`,
      limits: { demo: { rate: 1000, burst: 1 } }
    })
    queue = await RedisStreamQueue.connect({
      url: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
      prefix,
      maxQueueSize: 10
    })
  } catch (error) {
    t.skip(`Redis is not available: ${error.message}`)
    return
  }

  server = new GatewayServer({
    port: 0,
    queue,
    rateLimiter: limiter,
    adapters: {
      'demo/echo': async (payload, context) => {
        assert.deepEqual(context.request.headers, { 'X-Trace': '{{payload.trace}}' })
        return { echoed: payload }
      }
    }
  })
  await server.start()
  client = new GatewayClient({
    url: `ws://127.0.0.1:${server.httpServer.address().port}/ws`,
    podId: `redis-test-${randomUUID()}`
  })
  t.after(async () => {
    client.close()
    await server.stop()
  })

  const result = await client.request({
    type: 'call',
    operation: 'demo/echo',
    payload: { redis: true, trace: 'redis-request' },
    _request: { headers: { 'X-Trace': '{{payload.trace}}' } },
    deadlineAt: new Date(Date.now() + 5000).toISOString()
  }, 5000)
  assert.deepEqual(result.payload, { echoed: { redis: true, trace: 'redis-request' } })
})

test('real Redis queue deletes ACKed entries, promotes delayed retries and applies TTL', async t => {
  const prefix = `test-retention-${randomUUID()}`
  const url = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
  let inspector
  let queue
  let server
  let client
  let decisions = 0
  try {
    inspector = createClient({
      url,
      socket: { connectTimeout: 1000, reconnectStrategy: createReconnectStrategy() }
    })
    inspector.on('error', () => {})
    await inspector.connect()
    queue = await RedisStreamQueue.connect({
      url,
      prefix,
      maxQueueSize: 10,
      resultTtlSeconds: 7,
      retryPollMs: 10,
      blockMs: 10,
      deleteOnAck: true
    })
  } catch (error) {
    t.skip(`Redis is not available: ${error.message}`)
    await inspector?.quit().catch(() => {})
    return
  }
  server = new GatewayServer({
    port: 0,
    queue,
    rateLimiter: {
      allow: async () => {
        decisions++
        return decisions === 1 ? { allowed: false, retryAfterMs: 20 } : { allowed: true, retryAfterMs: 0 }
      }
    },
    adapters: { 'demo/echo': async payload => ({ echoed: payload }) }
  })
  await server.start()
  client = new GatewayClient({
    url: `ws://127.0.0.1:${server.httpServer.address().port}/ws`,
    podId: `redis-retention-${randomUUID()}`,
    reconnectInterval: 10
  })
  t.after(async () => {
    client.close()
    await server.stop()
    await inspector.quit().catch(() => {})
  })

  const idemKey = 'ttl-check'
  await queue.claimIdempotency('pod-ttl', idemKey, 'request-ttl')
  assert.ok((await inspector.ttl(queue._idempotencyKey('pod-ttl', idemKey))) >= 1)
  const result = await client.request({
    type: 'call',
    operation: 'demo/echo',
    payload: { delayed: true },
    deadlineAt: new Date(Date.now() + 5000).toISOString()
  }, 5000)
  assert.deepEqual(result.payload, { echoed: { delayed: true } })
  assert.equal(decisions, 2)

  const deadline = Date.now() + 2000
  let streamEntries = 1
  let delayedEntries = 1
  while (Date.now() < deadline && (streamEntries || delayedEntries || (await queue.depth()) !== 0)) {
    streamEntries = Number(await inspector.xLen(queue.streams.normal)) + Number(await inspector.xLen(queue.streams.bulk))
    delayedEntries = Number(await inspector.zCard(queue.retryStreams.normal)) + Number(await inspector.zCard(queue.retryStreams.bulk))
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert.equal(streamEntries, 0)
  assert.equal(delayedEntries, 0)
  assert.equal(await queue.depth(), 0)
})

test('concurrent scheduleRetry calls for the same request token do not double-count queue depth', async t => {
  const prefix = `test-race-${randomUUID()}`
  const url = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
  let queue
  try {
    queue = await RedisStreamQueue.connect({ url, prefix, maxQueueSize: 100 })
  } catch (error) {
    t.skip(`Redis is not available: ${error.message}`)
    return
  }
  t.after(() => queue.stop())

  // Same request token (same podId + requestId + priority) scheduled twice
  // "at once" -- this is what a stale reclaim racing a still-alive
  // consumer's own reschedule looks like from Redis's point of view.
  // SCHEDULE_RETRY_SCRIPT must decide check+increment+write atomically, so
  // exactly one of the two calls should count as "new" and increment depth.
  const item = { podId: 'pod-race', request: { requestId: 'racy-request', priority: 'normal' }, operation: { value: 'demo/echo' } }
  await Promise.all([
    queue.scheduleRetry({ ...item }, 5000),
    queue.scheduleRetry({ ...item }, 5000)
  ])

  assert.equal(await queue.depth(), 1)
  const member = queue._retryToken(item, 'normal')
  assert.equal(await queue.commandClient.zScore(queue.retryStreams.normal, member) != null, true)
  assert.equal(await queue.commandClient.hGet(queue.retryDataStreams.normal, member) != null, true)
})
