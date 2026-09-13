#!/usr/bin/env node
'use strict'

const { randomUUID } = require('node:crypto')
const { createClient } = require('redis')
const { GatewayServer } = require('../lib/gateway-server')
const { GatewayClient } = require('../lib/gateway-client')
const { RedisGcraRateLimiter } = require('../lib/redis-rate-limiter')
const { RedisStreamQueue } = require('../lib/redis-stream-queue')

/**
 * Exercise the real WebSocket Gateway and Redis Streams queue with a local
 * adapter. This intentionally does not call an external provider: the goal is
 * to measure gateway/Redis behavior without network variance or provider cost.
 *
 * Examples:
 *   npm run test:load
 *   LOAD_REQUESTS=50000 LOAD_RATE=1 LOAD_LIMIT=1 npm run test:load
 */
async function main () {
  const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
  const requests = positiveInteger(process.env.LOAD_REQUESTS, 200)
  const sendRate = nonNegativeNumber(process.env.LOAD_RATE, 200)
  const rateLimit = nonNegativeNumber(process.env.LOAD_LIMIT, 100000)
  const burst = positiveInteger(process.env.LOAD_BURST, Math.max(1, Math.ceil(rateLimit || 1)))
  const concurrency = positiveInteger(process.env.LOAD_CONCURRENCY, 32)
  const deadlineMs = positiveInteger(process.env.LOAD_DEADLINE_MS, 600000)
  const prefix = process.env.LOAD_PREFIX || `pod-gateway-load-${process.pid}-${randomUUID().slice(0, 8)}`
  const resultTtlSeconds = positiveInteger(process.env.REDIS_RESULT_TTL_SECONDS, 3600)
  const deleteOnAck = parseBoolean(process.env.REDIS_DELETE_ON_ACK, true)
  const retryPollMs = positiveInteger(process.env.REDIS_RETRY_POLL_MS, 25)
  const logger = quietLogger()

  let inspector
  let limiter
  let queue
  let server
  let client
  const startedAt = Date.now()
  try {
    inspector = createClient({ url: redisUrl, socket: { connectTimeout: 1000 } })
    inspector.on('error', () => {})
    await inspector.connect()
    limiter = await RedisGcraRateLimiter.connect({
      url: redisUrl,
      keyPrefix: `${prefix}:rl`,
      limits: rateLimit > 0 ? { 'load/echo': { rate: rateLimit, burst } } : {},
      logger
    })
    queue = await RedisStreamQueue.connect({
      url: redisUrl,
      prefix,
      maxQueueSize: Math.max(requests + 1, 10000),
      resultTtlSeconds,
      deleteOnAck,
      retryPollMs,
      blockMs: 25,
      batchSize: Math.min(128, Math.max(1, concurrency)),
      logger
    })
    server = new GatewayServer({
      port: 0,
      queue,
      rateLimiter: limiter,
      concurrency,
      resultTtlSeconds,
      logger,
      adapters: {
        'load/echo': async payload => ({ ok: true, index: payload.index })
      }
    })
    await server.start()
    client = new GatewayClient({
      url: `ws://127.0.0.1:${server.httpServer.address().port}/ws`,
      podId: `${prefix}-pod`,
      reconnectInterval: 25,
      logger
    })

    const before = await redisStats(inspector, queue)
    const peakMonitor = startPeakMonitor(inspector, queue, before)
    const requestPromises = []
    const intervalMs = sendRate > 0 ? 1000 / sendRate : 0
    for (let index = 0; index < requests; index++) {
      requestPromises.push(client.request({
        type: 'call',
        operation: 'load/echo',
        payload: { index },
        deadlineAt: new Date(Date.now() + deadlineMs).toISOString()
      }, deadlineMs))
      if (intervalMs > 0 && index + 1 < requests) await sleep(intervalMs)
    }
    const settled = await Promise.allSettled(requestPromises)
    const completed = settled.filter(result => result.status === 'fulfilled').length
    const failed = settled.length - completed
    const peak = await peakMonitor.stop()
    const after = await waitForIdle(inspector, queue, 5000)
    const durationMs = Date.now() - startedAt
    const report = {
      redisUrl: redactRedisUrl(redisUrl),
      prefix,
      requests,
      sendRate,
      configuredRateLimit: rateLimit,
      burst,
      completed,
      failed,
      durationMs,
      requestsPerSecond: Number((completed / Math.max(durationMs / 1000, 0.001)).toFixed(2)),
      before,
      peak,
      after,
      queueStats: queue.stats(),
      streamEntriesDeleted: deleteOnAck,
      delayedRetryKeys: queue.retryStreams
    }
    console.log(JSON.stringify(report, null, 2))
    if (failed) {
      const firstError = settled.find(result => result.status === 'rejected')?.reason
      console.error(`first failure: ${firstError?.code || 'ERROR'} ${firstError?.message || firstError}`)
      process.exitCode = 1
    }
  } finally {
    client?.close()
    if (server) await server.stop().catch(() => {})
    if (inspector?.isOpen) await inspector.quit().catch(() => {})
    // server.stop closes the queue and limiter that it owns. The explicit
    // fallbacks cover failures during startup before ownership is established.
    if (!server) {
      await queue?.stop?.().catch(() => {})
      await limiter?.close?.().catch(() => {})
    }
  }
}

async function waitForIdle (client, queue, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let stats
  do {
    stats = await redisStats(client, queue)
    if (stats.depth === 0 && stats.streamEntries === 0 && stats.delayedEntries === 0) return stats
    await sleep(25)
  } while (Date.now() < deadline)
  return stats
}

async function redisStats (client, queue) {
  const info = await client.info('memory')
  const usedMemory = Number(info.match(/(?:^|\n)used_memory:(\d+)/)?.[1] || 0)
  const streamEntries = Number(await client.xLen(queue.streams.normal)) + Number(await client.xLen(queue.streams.bulk))
  const delayedEntries = Number(await client.zCard(queue.retryStreams.normal)) + Number(await client.zCard(queue.retryStreams.bulk))
  return {
    usedMemory,
    depth: await queue.depth(),
    streamEntries,
    delayedEntries
  }
}

function startPeakMonitor (client, queue, initial, intervalMs) {
  let peak = { ...initial }
  let stopped = false
  let inFlight = null
  const sample = () => {
    if (stopped || inFlight) return
    inFlight = redisStats(client, queue)
      .then(stats => {
        peak = {
          usedMemory: Math.max(peak.usedMemory, stats.usedMemory),
          depth: Math.max(peak.depth, stats.depth),
          streamEntries: Math.max(peak.streamEntries, stats.streamEntries),
          delayedEntries: Math.max(peak.delayedEntries, stats.delayedEntries)
        }
      })
      .catch(() => {})
      .finally(() => { inFlight = null })
  }
  const timer = setInterval(sample, Math.max(10, Number(intervalMs) || 50))
  timer.unref?.()
  sample()
  return {
    async stop () {
      stopped = true
      clearInterval(timer)
      if (inFlight) await inFlight
      return peak
    }
  }
}

function quietLogger () {
  return { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} }
}

function positiveInteger (value, fallback) {
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback
}

function nonNegativeNumber (value, fallback) {
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback
}

function parseBoolean (value, fallback) {
  if (value === undefined || value === '') return fallback
  return value === true || value === 'true' || value === '1' || value === 1
}

function redactRedisUrl (value) {
  try {
    const url = new URL(value)
    if (url.password) url.password = '***'
    return url.toString()
  } catch {
    return '<redis-url>'
  }
}

function sleep (delayMs) {
  return new Promise(resolve => setTimeout(resolve, delayMs))
}

main().catch(error => {
  console.error(`[load-test] ${error.stack || error.message}`)
  process.exitCode = 1
})
