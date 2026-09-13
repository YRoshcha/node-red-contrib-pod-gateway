'use strict'

const { createHash, randomUUID } = require('node:crypto')
const { createClient } = require('redis')
const { normalizeRedisUrl } = require('./redis-url')
const { createReconnectStrategy, attachErrorLogger } = require('./redis-reconnect')

const DEFAULT_GROUP = 'pod-gateway'
const DEFAULT_PREFIX = 'podgw'
const DEFAULT_RESULT_TTL_SECONDS = 3600
const DEFAULT_RETRY_POLL_MS = 250

// Move one due retry from a sorted set and its payload hash to a Redis Stream
// in one server-side operation. The sorted-set member is a stable request
// token, so a gateway restart cannot create duplicate delayed retries for the
// same request.
// The gateway normally uses a single Redis primary, which also keeps these
// keys on the same Redis execution context.
const PROMOTE_RETRY_SCRIPT = `
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then return { 0 } end
local payload = redis.call('HGET', KEYS[2], ARGV[1])
if not payload then
  redis.call('ZREM', KEYS[1], ARGV[1])
  return { 0 }
end
local id = redis.call('XADD', KEYS[3], '*', 'payload', payload)
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return { 1, id }
`

// Check-existing-then-conditionally-increment-then-write was three separate
// round trips: two concurrent scheduleRetry calls for the same request token
// (the same Stream entry reclaimed via XAUTOCLAIM while a still-alive
// consumer is also rescheduling it, for example) could both read "not yet
// scheduled" before either had written, double-incrementing the queue depth
// for what becomes a single delayed-retry entry. Redis runs a script to
// completion before serving any other client's command, so folding the same
// three steps into one EVAL makes the whole decision atomic instead of just
// each individual command.
const SCHEDULE_RETRY_SCRIPT = `
local alreadyScheduled = redis.call('ZSCORE', KEYS[1], ARGV[1]) ~= false
local preserveDepth = ARGV[4] == '1'
if not alreadyScheduled and not preserveDepth then
  local depth = redis.call('INCR', KEYS[3])
  if tonumber(depth) > tonumber(ARGV[5]) then
    redis.call('DECR', KEYS[3])
    return { 0 }
  end
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return { 1, alreadyScheduled and 1 or 0 }
`

function safeHash (value) {
  return createHash('sha256').update(String(value)).digest('hex')
}

function positiveInteger (value, fallback) {
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback
}

/**
 * Redis Streams queue for the gateway. A consumer group is used even with one
 * gateway instance so pending entries can be reclaimed after a crash and the
 * queue can be scaled later without changing the message format.
 */
class RedisStreamQueue {
  constructor (options = {}) {
    // Keep blocking stream reads on their own connection. Redis does not
    // process another command on a connection while XREADGROUP is blocked;
    // sharing that socket with producers can make a request look queued even
    // though XADD is still waiting behind the consumer read.
    this.client = options.client
    this.commandClient = options.commandClient || options.producerClient || this.client
    this.group = options.group || DEFAULT_GROUP
    this.prefix = options.prefix || DEFAULT_PREFIX
    this.consumer = options.consumer || `${options.instanceId || randomUUID()}-${process.pid}`
    this.blockMs = Number(options.blockMs || 1000)
    this.batchSize = Number(options.batchSize || 16)
    this.maxQueueSize = Number(options.maxQueueSize || 10000)
    // Must exceed the maximum upstream timeout, otherwise a slow but healthy
    // consumer can be reclaimed while its API call is still running.
    this.claimIdleMs = Number(options.claimIdleMs || 120000)
    this.resultTtlSeconds = positiveInteger(options.resultTtlSeconds, DEFAULT_RESULT_TTL_SECONDS)
    this.deleteOnAck = options.deleteOnAck !== false
    this.retryPollMs = positiveInteger(options.retryPollMs, DEFAULT_RETRY_POLL_MS)
    this.retryBatchSize = positiveInteger(options.retryBatchSize, this.batchSize)
    this.logger = options.logger || console
    // These keys are deliberately NOT Redis Cluster hash-tagged (no `{...}`).
    // An earlier build did tag them (`<prefix>:{queue}:normal`) and that was
    // reverted -- see the Redis Cluster hash-tag note in README.md -- because
    // operators kept mistaking `{queue}` for a template variable to expand.
    // Re-tagging them is also not just a naming exercise: SCHEDULE_RETRY_SCRIPT
    // and PROMOTE_RETRY_SCRIPT each touch retryStream + retryDataStream +
    // depthKey in a single EVAL, and depthKey is shared across BOTH the
    // normal and bulk priorities. Redis Cluster requires every key an EVAL
    // touches to land in the same hash slot (CROSSSLOT otherwise), so tagging
    // normal/bulk differently (to spread load across slots, the usual reason
    // to tag at all) is incompatible with one shared depthKey -- that would
    // need depthKey split per priority first, changing what `Max queue size`
    // means today (currently one combined budget for normal+bulk together).
    // This queue already documents a single-Redis-primary assumption (see
    // PROMOTE_RETRY_SCRIPT above); do not add hash tags without solving the
    // depthKey/CROSSSLOT problem first, and re-verify against the README note
    // above before doing so.
    this.streams = {
      normal: `${this.prefix}:queue:normal`,
      bulk: `${this.prefix}:queue:bulk`
    }
    this.depthKey = `${this.prefix}:queue:depth`
    this.retryStreams = {
      normal: `${this.prefix}:retry:normal`,
      bulk: `${this.prefix}:retry:bulk`
    }
    this.retryDataStreams = {
      normal: `${this.prefix}:retry:data:normal`,
      bulk: `${this.prefix}:retry:data:bulk`
    }
    this.started = false
    this.closed = false
    this.loopPromise = null
    this.retryLoopPromise = null
    this.counters = {
      streamXAdd: 0,
      delayedScheduled: 0,
      delayedPromoted: 0,
      acknowledged: 0,
      deleted: 0
    }
  }

  static async connect (options = {}) {
    const redisUrl = normalizeRedisUrl(options.url || process.env.REDIS_URL || 'redis://127.0.0.1:6379')
    const reconnectStrategy = createReconnectStrategy()
    const client = options.client || createClient({
      url: redisUrl,
      // A request must fail fast when Redis is unavailable. Keeping commands
      // in node-redis' offline queue otherwise leaves the WebSocket caller
      // waiting until its unrelated 60-second request deadline.
      disableOfflineQueue: true,
      socket: {
        ...(options.socket || {}),
        connectTimeout: 1000,
        reconnectStrategy
      }
    })
    if (!options.client) attachErrorLogger(client, options.logger, 'redis queue', { strategy: reconnectStrategy })
    if (!client.isOpen) await client.connect()

    // A caller that supplies a client owns the connection shape and is
    // commonly using a fake client in tests, so preserve the shared-client
    // behaviour in that case. For normal connections, duplicate a producer
    // socket so XADD/SET/ACK are never behind a blocking XREADGROUP.
    let commandClient = options.commandClient || options.producerClient
    if (!commandClient && !options.client) {
      commandClient = client.duplicate()
      attachErrorLogger(commandClient, options.logger, 'redis queue (command)', { strategy: reconnectStrategy })
      await commandClient.connect()
    }
    const queue = new RedisStreamQueue({ ...options, client, commandClient })
    queue.logger.debug?.(`redis queue connected prefix=${queue.prefix} consumer=${queue.consumer}`)
    return queue
  }

  async start (itemHandler) {
    if (this.started) return
    if (!this.client?.isReady) throw new Error('Redis queue client is not ready')
    this.closed = false
    await this._ensureGroups()
    this.started = true
    this.logger.debug?.(`redis queue started normal=${this.streams.normal} bulk=${this.streams.bulk} group=${this.group}`)
    this.loopPromise = this._consumeLoop(itemHandler)
    this.retryLoopPromise = this._retryLoop()
  }

  // Used by the gateway's /healthz endpoint. Both sockets must be up: the
  // blocking consumer connection and the duplicated command connection used
  // for XADD/ACK/idempotency writes.
  isReady () {
    return Boolean(this.client?.isReady && this.commandClient?.isReady)
  }

  async enqueue (item) {
    if (!this.commandClient?.isReady) throw this._error('QUEUE_UNAVAILABLE', 'Redis queue is unavailable', true)
    const priority = item.request?.priority === 'bulk' ? 'bulk' : 'normal'
    const stream = this.streams[priority]
    const depth = await this.commandClient.incr(this.depthKey)
    if (Number(depth) > this.maxQueueSize) {
      await this.commandClient.decr(this.depthKey)
      throw this._error('QUEUE_FULL', 'Gateway queue is full', true)
    }
    let id
    try {
      id = await this.commandClient.xAdd(stream, '*', { payload: JSON.stringify(item) })
    } catch (error) {
      await this.commandClient.decr(this.depthKey).catch(() => {})
      throw error
    }
    this.counters.streamXAdd++
    this.logger.debug?.(`redis enqueue request=${item.request?.requestId || ''} stream=${stream} id=${id}`)
    return { id, stream, priority }
  }

  /**
   * Persist work that must be attempted later without appending a new Stream
   * entry on every rate-limit tick. By default this reserves one queue-depth
   * slot. A rate-limited item can pass preserveDepth=true because its original
   * Stream entry is still counted until the dispatcher ACKs it.
   */
  async scheduleRetry (item, delayMs = 0, { preserveDepth = false } = {}) {
    if (!this.commandClient?.isReady) throw this._error('QUEUE_UNAVAILABLE', 'Redis queue is unavailable', true)
    const priority = item.request?.priority === 'bulk' ? 'bulk' : 'normal'
    const retryStream = this.retryStreams[priority]
    const retryDataStream = this.retryDataStreams[priority]
    const delay = Number(delayMs)
    const retryAt = Date.now() + (Number.isFinite(delay) ? Math.max(0, delay) : 0)
    const sequence = Number(item.retrySequence || 0) + 1
    item.retrySequence = sequence
    const retryItem = { ...item, retrySequence: sequence, lastQueuedAt: Date.now() }
    delete retryItem.queueId
    delete retryItem.queueStream
    const member = this._retryToken(item, priority)
    const payload = JSON.stringify(retryItem)
    const result = await this.commandClient.eval(SCHEDULE_RETRY_SCRIPT, {
      keys: [retryStream, retryDataStream, this.depthKey],
      arguments: [member, payload, String(retryAt), preserveDepth ? '1' : '0', String(this.maxQueueSize)]
    })
    if (Number(result?.[0]) !== 1) {
      throw this._error('QUEUE_FULL', 'Gateway queue is full', true)
    }
    this.counters.delayedScheduled++
    this.logger.debug?.(`redis delayed retry request=${item.request?.requestId || ''} stream=${retryStream} retryAt=${retryAt} sequence=${sequence}`)
    return { retryStream, retryAt, sequence }
  }

  async claimIdempotency (podId, idempotencyKey, requestId, ttlSeconds = this.resultTtlSeconds) {
    if (!idempotencyKey) return { claimed: true }
    if (!this.commandClient?.isReady) throw this._error('QUEUE_UNAVAILABLE', 'Redis queue is unavailable', true)
    const key = this._idempotencyKey(podId, idempotencyKey)
    const created = await this.commandClient.set(key, JSON.stringify({ requestId }), { NX: true, EX: ttlSeconds })
    if (created === 'OK') {
      this.logger.debug?.(`redis idempotency claimed request=${requestId}`)
      return { claimed: true }
    }
    const existing = await this.commandClient.get(key)
    let value
    try { value = existing ? JSON.parse(existing) : null } catch { value = null }
    this.logger.debug?.(`redis idempotency duplicate request=${requestId} original=${value?.requestId || 'unknown'} cached=${Boolean(value?.response)}`)
    return { claimed: false, requestId: value?.requestId, response: value?.response }
  }

  async releaseIdempotency (podId, idempotencyKey, requestId) {
    if (!idempotencyKey) return
    const key = this._idempotencyKey(podId, idempotencyKey)
    const value = await this.commandClient.get(key)
    if (!value) return
    try {
      if (JSON.parse(value).requestId === requestId) await this.commandClient.del(key)
    } catch {}
  }

  async storeIdempotencyResult (podId, idempotencyKey, response, ttlSeconds = this.resultTtlSeconds) {
    if (!idempotencyKey) return
    await this.commandClient.set(this._idempotencyKey(podId, idempotencyKey), JSON.stringify({
      requestId: response.requestId,
      response
    }), { EX: ttlSeconds })
  }

  async getIdempotencyResult (podId, idempotencyKey) {
    if (!idempotencyKey) return null
    const value = await this.commandClient.get(this._idempotencyKey(podId, idempotencyKey))
    if (!value) return null
    try { return JSON.parse(value) } catch { return null }
  }

  async storePendingResult (podId, response) {
    const key = this._pendingKey(podId)
    await this.commandClient.hSet(key, response.requestId, JSON.stringify(response))
    await this.commandClient.expire(key, this.resultTtlSeconds)
  }

  async removePendingResult (podId, requestId) {
    await this.commandClient.hDel(this._pendingKey(podId), requestId)
  }

  async takePendingResults (podId) {
    const key = this._pendingKey(podId)
    const values = await this.commandClient.hGetAll(key)
    if (Object.keys(values).length) await this.commandClient.del(key)
    return Object.values(values).map(value => {
      try { return JSON.parse(value) } catch { return null }
    }).filter(Boolean)
  }

  async depth () {
    if (!this.commandClient?.isReady) return null
    return Number(await this.commandClient.get(this.depthKey) || 0)
  }

  stats () {
    return { ...this.counters }
  }

  async stop () {
    this.closed = true
    this.logger.debug?.('redis queue stopping')
    if (this.loopPromise) await this.loopPromise.catch(() => {})
    if (this.retryLoopPromise) await this.retryLoopPromise.catch(() => {})
    if (this.commandClient && this.commandClient !== this.client && this.commandClient.isOpen) {
      await this.commandClient.quit()
    }
    if (this.client?.isOpen) await this.client.quit()
    this.started = false
  }

  async _consumeLoop (itemHandler) {
    while (!this.closed) {
      try {
        const reclaimed = await this._claimPending()
        if (reclaimed.length) {
          await this._dispatch(reclaimed, itemHandler)
          continue
        }
        // Read normal traffic first. Bulk traffic is read only when there is no
        // immediately available normal message, preserving the intended priority.
        const normal = await this._read(this.streams.normal)
        if (normal.length) {
          await this._dispatch(normal, itemHandler)
          continue
        }
        const bulk = await this._read(this.streams.bulk)
        if (bulk.length) await this._dispatch(bulk, itemHandler)
      } catch (error) {
        if (!this.closed) {
          // Redis Streams can lose a key/group after an administrative delete,
          // failover or restore. Recreate the groups before retrying so one
          // transient NOGROUP does not permanently stall the consumer.
          if (String(error.message).includes('NOGROUP')) {
            try {
              await this._ensureGroups()
              continue
            } catch (groupError) {
              this.logger.error?.('[gateway] Redis queue group recovery error', groupError.message)
            }
          }
          this.logger.error?.('[gateway] Redis queue consumer error', error.message)
          await new Promise(resolve => setTimeout(resolve, 1000))
        }
      }
    }
  }

  async _retryLoop () {
    while (!this.closed) {
      let promoted = 0
      try {
        promoted = await this._promoteDueRetries()
      } catch (error) {
        if (!this.closed) this.logger.error?.('[gateway] Redis delayed retry error', error.message)
      }
      if (!this.closed && promoted === 0) {
        await new Promise(resolve => setTimeout(resolve, this.retryPollMs))
      }
    }
  }

  async _promoteDueRetries () {
    if (!this.commandClient?.isReady) return 0
    const now = Date.now()
    let promoted = 0
    for (const priority of ['normal', 'bulk']) {
      const retryStream = this.retryStreams[priority]
      const retryDataStream = this.retryDataStreams[priority]
      const targetStream = this.streams[priority]
      if (typeof this.commandClient.zRangeByScore !== 'function') continue
      const members = await this.commandClient.zRangeByScore(
        retryStream,
        '-inf',
        now,
        { LIMIT: { offset: 0, count: this.retryBatchSize } }
      )
      for (const member of members || []) {
        const result = await this._promoteRetryMember(retryStream, retryDataStream, targetStream, member, now)
        if (result?.promoted) promoted++
      }
    }
    if (promoted) this.logger.debug?.(`redis delayed retries promoted count=${promoted}`)
    return promoted
  }

  async _promoteRetryMember (retryStream, retryDataStream, targetStream, member, now) {
    if (typeof this.commandClient.eval === 'function') {
      const result = await this.commandClient.eval(PROMOTE_RETRY_SCRIPT, {
        keys: [retryStream, retryDataStream, targetStream],
        arguments: [member, String(now)]
      })
      const promoted = Number(result?.[0]) === 1
      if (promoted) {
        this.counters.delayedPromoted++
        this.counters.streamXAdd++
      }
      return { promoted, id: result?.[1] }
    }
    // Test doubles and compatible Redis clients may not expose EVAL. Keep a
    // best-effort fallback for those clients; production node-redis does.
    const payload = await this.commandClient.hGet(retryDataStream, member)
    const removed = await this.commandClient.zRem(retryStream, member)
    if (!removed) return { promoted: false }
    if (!payload) {
      await this.commandClient.hDel(retryDataStream, member).catch(() => {})
      return { promoted: false }
    }
    const id = await this.commandClient.xAdd(targetStream, '*', { payload })
    await this.commandClient.hDel(retryDataStream, member).catch(() => {})
    this.counters.delayedPromoted++
    this.counters.streamXAdd++
    return { promoted: true, id }
  }

  async _dispatch (messages, itemHandler) {
    await Promise.all(messages.map(async message => {
      try {
        const item = JSON.parse(message.message.payload)
        item.queueId = message.id
        item.queueStream = message.stream
        this.logger.debug?.(`redis dispatch request=${item.request?.requestId || ''} stream=${message.stream} id=${message.id}`)
        const outcome = await itemHandler(item, { id: message.id, stream: message.stream })
        await this.commandClient.xAck(message.stream, this.group, message.id)
        this.counters.acknowledged++
        if (this.deleteOnAck && typeof this.commandClient.xDel === 'function') {
          try {
            const deleted = await this.commandClient.xDel(message.stream, message.id)
            if (Number(deleted) > 0) this.counters.deleted++
          } catch (error) {
            // ACK has already made the item non-replayable. A deletion error
            // must not turn a completed request into a retry or leak depth.
            this.logger.warn?.(`[gateway] Redis stream entry retention cleanup failed id=${message.id}: ${error.message}`)
          }
        }
        if (!outcome?.deferred) await this.commandClient.decr(this.depthKey)
        this.logger.debug?.(`redis ACK request=${item.request?.requestId || ''} id=${message.id}`)
      } catch (error) {
        // No ACK: Redis keeps the entry pending and it will be reclaimed after
        // claimIdleMs. This protects tasks when a gateway process crashes.
        this.logger.error?.('[gateway] Redis queue item failed', error.message)
      }
    }))
  }

  async _read (stream) {
    const result = await this.client.xReadGroup(this.group, this.consumer, [{ key: stream, id: '>' }], {
      COUNT: this.batchSize,
      BLOCK: this.blockMs
    })
    return this._flatten(stream, result)
  }

  async _claimPending () {
    const all = []
    for (const stream of Object.values(this.streams)) {
      const result = await this.client.xAutoClaim(stream, this.group, this.consumer, this.claimIdleMs, '0-0', { COUNT: this.batchSize })
      all.push(...this._flatten(stream, [{ name: stream, messages: result.messages || [] }]))
    }
    return all
  }

  _flatten (stream, result) {
    if (!result) return []
    return result.flatMap(entry => (entry.messages || []).map(message => ({ ...message, stream: entry.name || stream })))
  }

  async _ensureGroup (stream) {
    try {
      await this.client.xGroupCreate(stream, this.group, '$', { MKSTREAM: true })
    } catch (error) {
      if (!String(error.message).includes('BUSYGROUP')) throw error
    }
  }

  async _ensureGroups () {
    await Promise.all(Object.values(this.streams).map(stream => this._ensureGroup(stream)))
  }

  _idempotencyKey (podId, idempotencyKey) {
    return `${this.prefix}:idem:${safeHash(`${podId}:${idempotencyKey}`)}`
  }

  _pendingKey (podId) {
    return `${this.prefix}:pending:${safeHash(podId)}`
  }

  _retryToken (item, priority) {
    return `${priority}:${item.podId || 'unknown'}:${item.request?.requestId || randomUUID()}`
  }

  _error (code, message, retryable = false) {
    const error = new Error(message)
    error.code = code
    error.retryable = retryable
    return error
  }
}

module.exports = { RedisStreamQueue, PROMOTE_RETRY_SCRIPT, SCHEDULE_RETRY_SCRIPT }
