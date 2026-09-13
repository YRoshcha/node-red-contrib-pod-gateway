'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { RedisStreamQueue, SCHEDULE_RETRY_SCRIPT } = require('../lib/redis-stream-queue')

test('uses readable Redis queue key names', () => {
  const queue = new RedisStreamQueue({ prefix: 'pod-gateway-full-demo' })

  assert.deepEqual(queue.streams, {
    normal: 'pod-gateway-full-demo:queue:normal',
    bulk: 'pod-gateway-full-demo:queue:bulk'
  })
  assert.equal(queue.depthKey, 'pod-gateway-full-demo:queue:depth')
})

test('creates missing consumer groups with the stream key', async () => {
  const calls = []
  const queue = new RedisStreamQueue({
    client: {
      xGroupCreate: async (...args) => calls.push(args)
    },
    prefix: 'test'
  })

  await queue._ensureGroup('test:queue:normal')

  assert.deepEqual(calls, [['test:queue:normal', 'pod-gateway', '$', { MKSTREAM: true }]])
})

test('fails idempotency claims when the producer connection is offline', async () => {
  const queue = new RedisStreamQueue({
    client: { isReady: false },
    prefix: 'test'
  })

  await assert.rejects(
    queue.claimIdempotency('pod-1', 'key-1', 'request-1'),
    error => error.code === 'QUEUE_UNAVAILABLE'
  )
})

test('enqueues normal and bulk work on separate streams', async () => {
  const calls = []
  const commandClient = {
    isReady: true,
    incr: async key => { calls.push(['incr', key]); return 1 },
    decr: async key => { calls.push(['decr', key]); return 0 },
    xAdd: async (...args) => { calls.push(['xAdd', ...args]); return '10-0' }
  }
  const queue = new RedisStreamQueue({ prefix: 'orders', commandClient, maxQueueSize: 10 })
  assert.deepEqual(await queue.enqueue({ request: { requestId: 'normal-1', priority: 'normal' } }), {
    id: '10-0', stream: 'orders:queue:normal', priority: 'normal'
  })
  assert.deepEqual(await queue.enqueue({ request: { requestId: 'bulk-1', priority: 'bulk' } }), {
    id: '10-0', stream: 'orders:queue:bulk', priority: 'bulk'
  })
  assert.equal(calls.filter(call => call[0] === 'xAdd').length, 2)
  assert.equal(queue.stats().streamXAdd, 2)
  const writes = calls.filter(call => call[0] === 'xAdd')
  assert.equal(JSON.parse(writes[0][3].payload).request.requestId, 'normal-1')
  assert.equal(JSON.parse(writes[1][3].payload).request.requestId, 'bulk-1')
})

test('rejects a full queue and rolls back depth', async () => {
  const calls = []
  const queue = new RedisStreamQueue({
    commandClient: {
      isReady: true,
      incr: async () => 3,
      decr: async key => calls.push(key),
      xAdd: async () => { throw new Error('must not write') }
    },
    prefix: 'full',
    maxQueueSize: 2
  })
  await assert.rejects(queue.enqueue({ request: { requestId: 'full-1' } }), error => error.code === 'QUEUE_FULL' && error.retryable)
  assert.deepEqual(calls, ['full:queue:depth'])
})

test('rolls back depth when Redis XADD fails', async () => {
  let decremented = 0
  const queue = new RedisStreamQueue({
    commandClient: {
      isReady: true,
      incr: async () => 1,
      decr: async () => { decremented++ },
      xAdd: async () => { throw new Error('write failed') }
    }
  })
  await assert.rejects(queue.enqueue({ request: { requestId: 'write-error' } }), /write failed/)
  assert.equal(decremented, 1)
})

test('claims, detects and releases idempotency records', async () => {
  let stored
  let deleted = 0
  const commandClient = {
    isReady: true,
    set: async (_key, value, options) => {
      stored = { value: JSON.parse(value), options }
      return stored.value.requestId === 'first' ? 'OK' : null
    },
    get: async () => JSON.stringify({ requestId: 'first', response: { ok: true } }),
    del: async () => { deleted++ }
  }
  const queue = new RedisStreamQueue({ commandClient, prefix: 'idem' })
  assert.deepEqual(await queue.claimIdempotency('pod', 'key', 'first'), { claimed: true })
  assert.equal(stored.options.NX, true)
  const duplicate = await queue.claimIdempotency('pod', 'key', 'second')
  assert.deepEqual(duplicate, { claimed: false, requestId: 'first', response: { ok: true } })
  await queue.releaseIdempotency('pod', 'key', 'first')
  assert.equal(deleted, 1)
  await queue.releaseIdempotency('pod', 'key', 'other')
  assert.equal(deleted, 1)
})

test('stores and reads idempotency and pending results', async () => {
  const records = new Map()
  let removedPending = false
  const commandClient = {
    isReady: true,
    set: async (key, value, options) => { records.set(key, { value, options }) },
    hSet: async (key, field, value) => {
      const entry = records.get(key) || { hash: {} }
      entry.hash = { ...(entry.hash || {}), [field]: value }
      records.set(key, entry)
    },
    expire: async (key, ttl) => { records.get(key).ttl = ttl },
    hGetAll: async key => records.get(key)?.hash || {},
    del: async key => { removedPending = key.includes(':pending:'); records.delete(key) },
    get: async key => records.get(key)?.value || null
  }
  const queue = new RedisStreamQueue({ commandClient, prefix: 'results' })
  await queue.storeIdempotencyResult('pod', 'key', { requestId: 'req', type: 'result', ok: true })
  assert.deepEqual((await queue.getIdempotencyResult('pod', 'key')).response, { requestId: 'req', type: 'result', ok: true })
  await queue.storePendingResult('pod', { requestId: 'pending', ok: true })
  assert.deepEqual(await queue.takePendingResults('pod'), [{ requestId: 'pending', ok: true }])
  assert.equal(removedPending, true)
  assert.deepEqual(await queue.takePendingResults('pod'), [])
  assert.equal(await queue.depth(), 0)
})

test('uses the configured idempotency TTL for claims and results', async () => {
  const calls = []
  const commandClient = {
    isReady: true,
    set: async (...args) => { calls.push(args); return 'OK' },
    hSet: async () => {},
    expire: async (...args) => calls.push(args)
  }
  const queue = new RedisStreamQueue({ commandClient, prefix: 'ttl', resultTtlSeconds: 17 })
  await queue.claimIdempotency('pod', 'key', 'request')
  await queue.storeIdempotencyResult('pod', 'key', { requestId: 'request', ok: true })
  await queue.storePendingResult('pod', { requestId: 'pending', ok: true })
  assert.equal(calls[0][2].EX, 17)
  assert.equal(calls[1][2].EX, 17)
  assert.equal(calls[2][1], 17)
})

test('stores delayed retries once (one atomic EVAL) and promotes due work to the target Stream', async () => {
  const calls = []
  let member
  const commandClient = {
    isReady: true,
    zRangeByScore: async (...args) => { calls.push(['zRangeByScore', ...args]); return String(args[0]).endsWith(':normal') && member ? [member] : [] },
    eval: async (script, options) => {
      calls.push(['eval', script, options])
      if (script === SCHEDULE_RETRY_SCRIPT) {
        member = options.arguments[0]
        return [1, 0]
      }
      return [1, '20-0']
    }
  }
  const queue = new RedisStreamQueue({ commandClient, prefix: 'retry', retryPollMs: 10 })
  const item = { podId: 'pod', request: { requestId: 'req', priority: 'normal' }, operation: { value: 'demo/echo' } }
  const scheduled = await queue.scheduleRetry(item, 0)
  assert.equal(scheduled.retryStream, 'retry:retry:normal')
  const evalCalls = calls.filter(call => call[0] === 'eval')
  // scheduleRetry must be a single round trip -- the check, the depth
  // accounting and the write all happen inside one EVAL, not as three
  // separate commands a concurrent caller could interleave with.
  assert.equal(evalCalls.length, 1)
  const scheduleCall = evalCalls[0]
  assert.equal(scheduleCall[1], SCHEDULE_RETRY_SCRIPT)
  assert.equal(scheduleCall[2].keys[0], 'retry:retry:normal')
  assert.equal(scheduleCall[2].keys[1], 'retry:retry:data:normal')
  assert.equal(scheduleCall[2].keys[2], 'retry:queue:depth')
  await queue._promoteDueRetries()
  const promoteCall = calls.find(call => call[0] === 'eval' && call[1] !== SCHEDULE_RETRY_SCRIPT)
  assert.equal(promoteCall[2].keys[0], 'retry:retry:normal')
  assert.equal(promoteCall[2].keys[1], 'retry:retry:data:normal')
  assert.equal(promoteCall[2].keys[2], 'retry:queue:normal')
  assert.equal(promoteCall[2].arguments[0], member)
  assert.equal(queue.stats().delayedScheduled, 1)
  assert.equal(queue.stats().delayedPromoted, 1)
  assert.equal(queue.stats().streamXAdd, 1)
})

test('deduplicates a delayed retry for the same request after a crash window', async () => {
  const evalCalls = []
  const commandClient = {
    isReady: true,
    // alreadyScheduled=1 in the return value is what SCHEDULE_RETRY_SCRIPT
    // reports when ZSCORE already found this member -- depth was never
    // touched inside the script in that case.
    eval: async (script, options) => { evalCalls.push([script, options]); return [1, 1] }
  }
  const queue = new RedisStreamQueue({ commandClient, prefix: 'retry-dedupe' })
  await queue.scheduleRetry({ podId: 'pod', request: { requestId: 'same' } }, 10)
  assert.equal(evalCalls.length, 1)
  assert.equal(queue.stats().delayedScheduled, 1)
})

test('rejects a full retry queue without double-counting a race between two concurrent schedulers', async () => {
  // Two overlapping scheduleRetry calls for the same token must not each
  // independently see "not yet scheduled" and both increment queue depth --
  // the whole check+increment+write now happens inside SCHEDULE_RETRY_SCRIPT,
  // so from the JS side it is just one EVAL call whose own return value
  // (index 0) says whether it was accepted (1) or the queue was full (0).
  const commandClient = {
    isReady: true,
    eval: async () => [0]
  }
  const queue = new RedisStreamQueue({ commandClient, prefix: 'retry-full', maxQueueSize: 1 })
  await assert.rejects(
    queue.scheduleRetry({ podId: 'pod', request: { requestId: 'over-limit' } }, 10),
    error => error.code === 'QUEUE_FULL'
  )
})

test('ACKs and deletes successful entries without decrementing transferred depth', async () => {
  const calls = []
  const queue = new RedisStreamQueue({
    commandClient: {
      xAck: async (...args) => calls.push(['ack', ...args]),
      xDel: async (...args) => calls.push(['xdel', ...args]),
      decr: async (...args) => calls.push(['decr', ...args])
    },
    prefix: 'retention'
  })
  await queue._dispatch([{ id: '1-0', stream: 'retention:queue:normal', message: { payload: JSON.stringify({ request: { requestId: 'req' } }) } }], async () => ({ deferred: true }))
  assert.deepEqual(calls.map(call => call[0]), ['ack', 'xdel'])
  assert.equal(queue.stats().acknowledged, 1)
})

test('can retain acknowledged Stream entries for short debugging sessions', async () => {
  let deleted = 0
  const queue = new RedisStreamQueue({
    deleteOnAck: false,
    commandClient: {
      xAck: async () => {},
      xDel: async () => { deleted++ },
      decr: async () => {}
    }
  })
  await queue._dispatch([{ id: '1-0', stream: 'q', message: { payload: JSON.stringify({ request: { requestId: 'req' } }) } }], async () => {})
  assert.equal(deleted, 0)
  assert.equal(queue.stats().acknowledged, 1)
})

test('maps stream reads and ACKs successful dispatches', async () => {
  const calls = []
  const queue = new RedisStreamQueue({
    client: {
      isReady: true,
      xReadGroup: async (...args) => {
        calls.push(['read', ...args])
        return [{ name: 'q:normal', messages: [{ id: '1-0', message: { payload: JSON.stringify({ request: { requestId: 'req' } }) } }] }]
      },
      xAutoClaim: async () => ({ messages: [] })
    },
    commandClient: {
      isReady: true,
      xAck: async (...args) => calls.push(['ack', ...args]),
      decr: async (...args) => calls.push(['decr', ...args])
    },
    prefix: 'q'
  })
  const messages = []
  await queue._dispatch(await queue._read(queue.streams.normal), async item => messages.push(item))
  assert.equal(messages[0].queueId, '1-0')
  assert.equal(messages[0].queueStream, 'q:normal')
  assert.deepEqual(calls[1], ['ack', 'q:normal', 'pod-gateway', '1-0'])
  assert.deepEqual(calls[2], ['decr', 'q:queue:depth'])
})

test('does not ACK an invalid or failed queue item', async () => {
  let acked = 0
  const queue = new RedisStreamQueue({
    commandClient: { xAck: async () => { acked++ }, decr: async () => {} },
    logger: { error: () => {} }
  })
  await queue._dispatch([{ id: 'bad', stream: 'q', message: { payload: '{bad' } }], async () => {})
  await queue._dispatch([{ id: 'failed', stream: 'q', message: { payload: '{}' } }], async () => { throw new Error('handler failed') })
  assert.equal(acked, 0)
})

test('ignores an existing consumer group but propagates other group errors', async () => {
  const queue = new RedisStreamQueue({ client: { xGroupCreate: async () => { throw new Error('BUSYGROUP exists') } } })
  await queue._ensureGroup('q')
  const failed = new RedisStreamQueue({ client: { xGroupCreate: async () => { throw new Error('permission denied') } } })
  await assert.rejects(failed._ensureGroup('q'), /permission denied/)
})

test('returns null depth while offline and closes both Redis connections', async () => {
  let quit = 0
  const client = { isReady: false, isOpen: true, quit: async () => { quit++ } }
  const commandClient = { isOpen: true, quit: async () => { quit++ } }
  const queue = new RedisStreamQueue({ client, commandClient })
  assert.equal(await queue.depth(), null)
  await queue.stop()
  assert.equal(quit, 2)
})
