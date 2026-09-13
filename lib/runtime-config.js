'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { loadDotEnv } = require('./env')

/**
 * Load the central gateway configuration.
 *
 * The JSON file is the primary configuration surface. Environment variables
 * remain supported as deployment-time overrides, which is useful for secrets
 * injected by Docker/Kubernetes.
 */
function loadGatewayConfig (file) {
  loadDotEnv()

  const configPath = file || process.env.POD_GATEWAY_CONFIG || path.join(process.cwd(), 'gateway.config.json')
  const fileConfig = readJsonIfPresent(configPath)
  const redis = fileConfig.redis || {}
  const server = fileConfig.server || {}
  const auth = fileConfig.auth || {}
  const upstream = fileConfig.upstream || {}

  return {
    configPath,
    redis: {
      url: process.env.REDIS_URL || redis.url || 'redis://127.0.0.1:6379',
      keyPrefix: process.env.REDIS_KEY_PREFIX || redis.keyPrefix || 'pod-gateway',
      maxQueueSize: number(process.env.GATEWAY_MAX_QUEUE, redis.maxQueueSize, 10000),
      resultTtlSeconds: positiveInteger(process.env.REDIS_RESULT_TTL_SECONDS, redis.resultTtlSeconds, 3600),
      deleteOnAck: boolean(process.env.REDIS_DELETE_ON_ACK, redis.deleteOnAck, true),
      retryPollMs: positiveInteger(process.env.REDIS_RETRY_POLL_MS, redis.retryPollMs, 250)
    },
    server: {
      port: number(process.env.PORT, server.port, 8080),
      concurrency: number(process.env.GATEWAY_CONCURRENCY, server.concurrency, 32)
    },
    auth: {
      token: process.env.GATEWAY_TOKEN || auth.token || ''
    },
    rateLimits: loadJsonEnv(process.env.GATEWAY_RATE_LIMITS, parseJsonObject(fileConfig.rateLimits, {}, 'rateLimits')),
    upstream: {
      baseUrl: process.env.UPSTREAM_BASE_URL || upstream.baseUrl || '',
      path: process.env.UPSTREAM_PATH || upstream.path || '/',
      method: process.env.UPSTREAM_METHOD || upstream.method || 'GET',
      apiKey: process.env.UPSTREAM_API_KEY || upstream.apiKey || '',
      apiKeyHeader: process.env.UPSTREAM_API_KEY_HEADER || upstream.apiKeyHeader || 'Authorization',
      apiKeyPrefix: process.env.UPSTREAM_API_KEY_PREFIX || upstream.apiKeyPrefix || 'Bearer ',
      rate: number(process.env.UPSTREAM_RATE, upstream.rate, 50),
      burst: number(process.env.UPSTREAM_BURST, upstream.burst, 1)
    }
  }
}

function readJsonIfPresent (file) {
  if (!fs.existsSync(file)) return {}
  let parsed
  try {
    parsed = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch (error) {
    throw new Error(`Invalid gateway config ${file}: ${error.message}`)
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`Gateway config ${file} must contain a JSON object`)
  }
  return parsed
}

function loadJsonEnv (value, fallback) {
  if (!value) return fallback
  return parseJsonObject(value, fallback, 'GATEWAY_RATE_LIMITS JSON')
}

function parseJsonObject (value, fallback, label) {
  if (value === undefined || value === null || value === '') return fallback
  const parsed = typeof value === 'string'
    ? (() => {
        try { return JSON.parse(value) } catch (error) { throw new Error(`Invalid ${label}: ${error.message}`) }
      })()
    : value
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`Invalid ${label}: must be a JSON object`)
  }
  return parsed
}

function number (envValue, fileValue, fallback) {
  const value = envValue ?? fileValue
  if (value === undefined || value === '') return fallback
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) throw new Error(`Expected a number, received ${value}`)
  return parsed
}

function positiveInteger (envValue, fileValue, fallback) {
  const parsed = number(envValue, fileValue, fallback)
  if (!Number.isInteger(parsed) || parsed < 1) throw new Error(`Expected a positive integer, received ${parsed}`)
  return parsed
}

function boolean (envValue, fileValue, fallback) {
  const value = envValue ?? fileValue
  if (value === undefined || value === '') return fallback
  if (value === true || value === 'true' || value === 1 || value === '1') return true
  if (value === false || value === 'false' || value === 0 || value === '0') return false
  throw new Error(`Expected a boolean, received ${value}`)
}

module.exports = { loadGatewayConfig }
