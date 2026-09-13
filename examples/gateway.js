'use strict'

const {
  GatewayServer,
  RedisGcraRateLimiter,
  RedisStreamQueue,
  createJsonHttpAdapter,
  requestContract
} = require('..')
const { loadGatewayConfig } = require('../lib/runtime-config')

async function main () {
  const config = loadGatewayConfig()
  const upstream = config.upstream
  const apiBaseUrl = upstream.baseUrl
  const apiKey = upstream.apiKey
  const upstreamLimit = config.rateLimits.upstream || {
    rate: upstream.rate,
    burst: upstream.burst
  }
  if (!apiBaseUrl || !apiKey) {
    throw new Error('Set upstream.baseUrl and upstream.apiKey in gateway.config.json before starting the example gateway')
  }

  const rateLimiter = await RedisGcraRateLimiter.connect({
    url: config.redis.url,
    keyPrefix: config.redis.keyPrefix,
    limits: {
      upstream: upstreamLimit
    }
  })
  const queue = await RedisStreamQueue.connect({
    url: config.redis.url,
    prefix: config.redis.keyPrefix,
    maxQueueSize: config.redis.maxQueueSize
  })

  const server = new GatewayServer({
    port: config.server.port,
    rateLimiter,
    queue,
    maxQueueSize: config.redis.maxQueueSize,
    concurrency: config.server.concurrency,
    authenticate: async message => !config.auth.token || message.token === config.auth.token
  })

  server.registerOperation('upstream/request', createJsonHttpAdapter({
    url: `${apiBaseUrl.replace(/\/$/, '')}${upstream.path || '/'}`,
    method: upstream.method || 'GET',
    headers: () => ({
      [upstream.apiKeyHeader || 'Authorization']: `${upstream.apiKeyPrefix || 'Bearer '}${apiKey}`
    })
  }), {
    label: 'External API request',
    description: 'Call the configured upstream API',
    requestSchema: requestContract.requestSchema(upstream.method || 'GET', upstream.path || '/')
  })

  await server.start()
  console.log(`POD Gateway listening on ${config.server.port}`)
  const shutdown = async () => {
    await server.stop()
    process.exit(0)
  }
  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
}

main().catch(error => {
  console.error(error.stack || error.message)
  process.exitCode = 1
})
