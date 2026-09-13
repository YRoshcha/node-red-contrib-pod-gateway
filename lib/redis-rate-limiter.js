'use strict'

const { createClient } = require('redis')
const { normalizeRedisUrl } = require('./redis-url')
const { createReconnectStrategy, attachErrorLogger } = require('./redis-reconnect')

const GCRA_SCRIPT = `
local n = tonumber(ARGV[1])
local cost = tonumber(ARGV[#ARGV])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000.0 + tonumber(t[2]) / 1000.0
local newTat = {}
local worstWait = 0

for i = 1, n do
  local rate = tonumber(ARGV[i * 2])
  local burst = tonumber(ARGV[i * 2 + 1])
  if not rate or rate <= 0 then return { 0, 0 } end
  local emission = 1000.0 / rate
  local delayTol = emission * burst
  local tat = tonumber(redis.call('HGET', KEYS[i], 'tat')) or now
  if tat < now then tat = now end
  local nextTat = tat + emission * cost
  local allowAt = nextTat - delayTol
  if allowAt > now and allowAt - now > worstWait then
    worstWait = allowAt - now
  end
  newTat[i] = nextTat
end

if worstWait > 0 then return { 0, math.ceil(worstWait) } end

for i = 1, n do
  local rate = tonumber(ARGV[i * 2])
  local burst = tonumber(ARGV[i * 2 + 1])
  local ttl = math.ceil((1000.0 / rate) * burst * 2) + 1000
  redis.call('HSET', KEYS[i], 'tat', newTat[i])
  redis.call('PEXPIRE', KEYS[i], ttl)
end

return { 1, 0 }
`

class RedisGcraRateLimiter {
  constructor ({ client, limits = {}, keyPrefix = 'rl', logger = console }) {
    this.client = client
    this.limits = limits && typeof limits === 'object' ? limits : {}
    this.keyPrefix = keyPrefix
    this.logger = logger || console
  }

  static async connect ({ url, limits, keyPrefix, socket, logger } = {}) {
    const redisUrl = normalizeRedisUrl(url || process.env.REDIS_URL || 'redis://127.0.0.1:6379')
    const reconnectStrategy = createReconnectStrategy()
    const client = createClient({
      url: redisUrl,
      disableOfflineQueue: true,
      socket: {
        ...(socket || {}),
        connectTimeout: 1000,
        reconnectStrategy
      }
    })
    attachErrorLogger(client, logger, 'redis rate limiter', { strategy: reconnectStrategy })
    await client.connect()
    const limiter = new RedisGcraRateLimiter({ client, limits, keyPrefix, logger })
    limiter.logger.debug?.(`redis rate limiter connected prefix=${keyPrefix}`)
    return limiter
  }

  async allow (service, cost = 1, operation = '', operationLimit) {
    const selected = operationLimit
      ? { config: operationLimit, bucket: String(operation || service || '').trim() }
      : selectLimit(this.limits, service, operation)
    const configured = selected.config
    if (!configured || !configured.rate) {
      this.logger.debug?.(`rate limit bypass service=${service} operation=${operation || ''} bucket=${selected.bucket || ''}`)
      return { allowed: true, retryAfterMs: 0 }
    }
    if (!this.client.isReady) {
      const error = new Error('Redis rate limiter is unavailable')
      error.code = 'RATE_LIMITER_UNAVAILABLE'
      throw error
    }
    const key = `${this.keyPrefix}:{${selected.bucket}}:g`
    this.logger.debug?.(`rate limit check service=${service} operation=${operation || ''} bucket=${selected.bucket} rate=${configured.rate} burst=${configured.burst ?? 1}`)
    const result = await this.client.eval(GCRA_SCRIPT, {
      keys: [key],
      arguments: ['1', String(configured.rate), String(configured.burst ?? 1), String(cost)]
    })
    const decision = { allowed: Number(result[0]) === 1, retryAfterMs: Number(result[1]) || 0 }
    this.logger.debug?.(`rate limit result service=${service} operation=${operation || ''} bucket=${selected.bucket} allowed=${decision.allowed} retryAfterMs=${decision.retryAfterMs}`)
    return decision
  }

  async close () {
    if (this.client?.isOpen) await this.client.quit()
  }
}

function selectLimit (limits, service, operation) {
  const operationKey = String(operation || '').trim()
  const serviceKey = String(service || '').trim()
  if (operationKey && Object.prototype.hasOwnProperty.call(limits, operationKey)) {
    return { config: limits[operationKey], bucket: operationKey }
  }
  if (serviceKey && Object.prototype.hasOwnProperty.call(limits, serviceKey)) {
    return { config: limits[serviceKey], bucket: serviceKey }
  }
  if (Object.prototype.hasOwnProperty.call(limits, 'default')) {
    return { config: limits.default, bucket: 'default' }
  }
  return { config: null, bucket: serviceKey || operationKey }
}

module.exports = { RedisGcraRateLimiter, GCRA_SCRIPT }
