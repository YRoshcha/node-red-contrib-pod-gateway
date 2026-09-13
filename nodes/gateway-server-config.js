'use strict'

const { GatewayServer } = require('../lib/gateway-server')
const { RedisGcraRateLimiter } = require('../lib/redis-rate-limiter')
const { RedisStreamQueue } = require('../lib/redis-stream-queue')
const { normalizeRedisUrl } = require('../lib/redis-url')
const { createLogger } = require('../lib/logger')

module.exports = function (RED) {
  function GatewayServerConfigNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    const logger = createLogger(RED.log, { verbose: config.verboseLogging })
    node.logger = logger
    let redisUrl
    let configError = null
    try {
      redisUrl = normalizeRedisUrl(config.redisUrl || 'redis://127.0.0.1:6379')
    } catch (error) {
      configError = error
      redisUrl = String(config.redisUrl || '')
    }
    const keyPrefix = config.redisKeyPrefix || 'pod-gateway'
    const maxQueueSize = Number(config.redisMaxQueueSize || 10000)
    const resultTtlSeconds = positiveInteger(config.redisResultTtlSeconds, 3600)
    const deleteOnAck = config.redisDeleteOnAck !== false && config.redisDeleteOnAck !== 'false'
    const retryPollMs = positiveInteger(config.redisRetryPollMs, 250)
    const concurrency = Number(config.concurrency || 32)
    const upstreamTimeoutMs = Number(config.upstreamTimeoutMs || 60000)
    // claimIdleMs (how long an unacknowledged Redis Stream entry sits before
    // XAUTOCLAIM reclaims it) is not exposed as its own editor field. It must
    // still exceed the slowest configured upstream call, or a slow-but-
    // healthy request gets reclaimed and dispatched a second time while the
    // first attempt is still running -- so derive a safe floor from the
    // configured timeout instead of leaving it hardcoded at 120s regardless
    // of what the operator sets upstreamTimeoutMs to.
    const claimIdleMarginMs = 30000
    const defaultClaimIdleMs = 120000
    const claimIdleMs = Math.max(defaultClaimIdleMs, upstreamTimeoutMs + claimIdleMarginMs)
    const port = Number(config.port || 8080)
    const token = node.credentials?.token || config.token || ''
    const redisSocket = redisUrl.startsWith('rediss://')
      ? { rejectUnauthorized: config.redisRejectUnauthorized !== false && config.redisRejectUnauthorized !== 'false' }
      : undefined
    let limits
    try {
      limits = parseRateLimits(config.rateLimits)
    } catch (error) {
      configError = configError || error
      limits = {}
    }
    let limiter
    let queue

    node.server = null
    node.ready = configError ? Promise.reject(configError) : start()
    // Node-RED does not await promises returned from node constructors. Keep
    // the rejection observable through node status/logging without taking the
    // whole Node-RED process down when Redis is unavailable.
    node.ready.catch(() => {})
    node.registerOperation = async (...args) => {
      await node.ready
      node.server.registerOperation(...args)
    }
    node.unregisterOperation = async operation => {
      await node.ready
      return node.server.unregisterOperation(operation)
    }

    if (configError) {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid configuration' })
      node.error(configError.message)
    }

    async function start () {
      node.status({ fill: 'yellow', shape: 'ring', text: 'connecting to Redis' })
      try {
        limiter = await RedisGcraRateLimiter.connect({
          url: redisUrl,
          keyPrefix,
          limits,
          socket: redisSocket,
          logger
        })
        queue = await RedisStreamQueue.connect({
          url: redisUrl,
          prefix: keyPrefix,
          maxQueueSize,
          resultTtlSeconds,
          deleteOnAck,
          retryPollMs,
          claimIdleMs,
          consumer: config.redisConsumer || `gateway-${node.id}`,
          socket: redisSocket,
          logger
        })
        node.server = new GatewayServer({
          host: config.host || '0.0.0.0',
          port,
          path: config.path || '/ws',
          rateLimiter: limiter,
          queue,
          maxQueueSize,
          concurrency,
          resultTtlSeconds,
          upstreamTimeoutMs,
          maxAttempts: Number(config.maxAttempts || 0),
          authenticate: async message => !token || message.token === token,
          logger
        })
        node.serverMetricListener = metric => node.emit('gateway-metric', metric)
        node.server.on('metric', node.serverMetricListener)
        await node.server.start()
        node.status({ fill: 'green', shape: 'dot', text: `listening on ${port}` })
      } catch (error) {
        node.status({ fill: 'red', shape: 'ring', text: error.code || 'startup failed' })
        node.error(`Gateway server startup failed: ${error.message}`)
        throw error
      }
    }

    node.on('close', async (_removed, done) => {
      try {
        await node.ready.catch(() => {})
        if (node.server && node.serverMetricListener) {
          node.server.removeListener('metric', node.serverMetricListener)
        }
        if (node.server) await node.server.stop()
        else {
          await queue?.stop?.().catch(() => {})
          await limiter?.close?.().catch(() => {})
        }
      } finally {
        if (typeof done === 'function') done()
      }
    })
  }

  RED.nodes.registerType('gateway-server-config', GatewayServerConfigNode, {
    credentials: {
      token: { type: 'password' }
    }
  })
}

function positiveInteger (value, fallback) {
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback
}

function parseRateLimits (value) {
  if (!value) return {}
  if (typeof value === 'object') return value
  try {
    const parsed = JSON.parse(value)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('must be a JSON object')
    return parsed
  } catch (error) {
    throw new Error(`Invalid rate limits JSON: ${error.message}`)
  }
}
