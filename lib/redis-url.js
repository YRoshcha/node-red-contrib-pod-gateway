'use strict'

function normalizeRedisUrl (value) {
  const raw = String(value || 'redis://127.0.0.1:6379').trim()
  if (!raw.includes('://')) return `redis://${raw}`
  if (!/^rediss?:\/\//i.test(raw)) {
    throw new Error('Redis URL must start with redis:// or rediss://')
  }
  return raw
}

module.exports = { normalizeRedisUrl }
