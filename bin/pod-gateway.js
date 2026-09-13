#!/usr/bin/env node
'use strict'

const { GatewayServer } = require('../lib/gateway-server')
const { RedisGcraRateLimiter } = require('../lib/redis-rate-limiter')
const { RedisStreamQueue } = require('../lib/redis-stream-queue')
const { loadGatewayConfig } = require('../lib/runtime-config')

async function main () {
  const config = loadGatewayConfig()
  let rateLimiter
  let queue
  try {
    rateLimiter = await RedisGcraRateLimiter.connect({
      url: config.redis.url,
      keyPrefix: config.redis.keyPrefix,
      limits: config.rateLimits
    })
    queue = await RedisStreamQueue.connect({
      url: config.redis.url,
      prefix: config.redis.keyPrefix,
      maxQueueSize: config.redis.maxQueueSize,
      resultTtlSeconds: config.redis.resultTtlSeconds,
      deleteOnAck: config.redis.deleteOnAck,
      retryPollMs: config.redis.retryPollMs
    })
  } catch (error) {
    console.error(`[gateway] Redis is unavailable: ${error.message}`)
    process.exitCode = 1
    return
  }

  const server = new GatewayServer({
    port: config.server.port,
    rateLimiter,
    queue,
    maxQueueSize: config.redis.maxQueueSize,
    concurrency: config.server.concurrency,
    resultTtlSeconds: config.redis.resultTtlSeconds,
    logger: console,
    authenticate: async message => {
      const expected = config.auth.token
      return !expected || message.token === expected
    }
  })

  // Operations are intentionally registered by the application that embeds the
  // gateway. The standalone command is a safe shell until adapters are added.
  await server.start()
  console.log(`[gateway] listening on ${config.server.port}`)

  const shutdown = async () => {
    await server.stop()
    process.exit(0)
  }
  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
}

main().catch(error => {
  console.error(`[gateway] startup failed: ${error.stack || error.message}`)
  process.exitCode = 1
})
