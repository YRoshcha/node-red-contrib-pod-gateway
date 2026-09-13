'use strict'

// Deterministic limiter used by tests and by local development only. Production
// should use RedisGcraRateLimiter so the gateway budget survives restarts and
// remains authoritative when the deployment grows later.
class InMemoryGcraRateLimiter {
  constructor (limits = {}) {
    this.limits = limits && typeof limits === 'object' ? limits : {}
    this.state = new Map()
  }

  async allow (service, cost = 1, operation = '', operationLimit) {
    const selected = operationLimit
      ? { config: operationLimit, bucket: String(operation || service || '').trim() }
      : selectLimit(this.limits, service, operation)
    const configured = selected.config
    if (!configured || !configured.rate) return { allowed: true, retryAfterMs: 0 }

    const now = Date.now()
    const rate = Number(configured.rate)
    const burst = Number(configured.burst ?? 1)
    const emission = 1000 / rate
    const delayTolerance = emission * burst
    const bucket = selected.bucket || service
    let tat = this.state.get(bucket) || now
    if (tat < now) tat = now
    const newTat = tat + emission * cost
    const allowAt = newTat - delayTolerance
    if (allowAt > now) {
      return { allowed: false, retryAfterMs: Math.ceil(allowAt - now) }
    }
    this.state.set(bucket, newTat)
    return { allowed: true, retryAfterMs: 0 }
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

module.exports = { InMemoryGcraRateLimiter }
