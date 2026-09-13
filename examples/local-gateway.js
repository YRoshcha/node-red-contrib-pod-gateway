'use strict'

const {
  GatewayServer,
  RedisGcraRateLimiter,
  RedisStreamQueue,
  requestContract
} = require('..')
const { loadGatewayConfig } = require('../lib/runtime-config')

async function main () {
  const config = loadGatewayConfig()
  const prefix = config.redis.keyPrefix
  const limiter = await RedisGcraRateLimiter.connect({
    url: config.redis.url,
    keyPrefix: prefix,
    limits: config.rateLimits
  })
  const queue = await RedisStreamQueue.connect({
    url: config.redis.url,
    prefix,
    maxQueueSize: config.redis.maxQueueSize
  })
  const gateway = new GatewayServer({
    port: config.server.port,
    rateLimiter: limiter,
    queue,
    maxQueueSize: config.redis.maxQueueSize,
    concurrency: config.server.concurrency,
    authenticate: async message => !config.auth.token || message.token === config.auth.token
  })

  gateway.registerOperation('demo/echo', async payload => {
    await new Promise(resolve => setTimeout(resolve, 25))
    return { echoed: payload, handledBy: 'central-gateway' }
  }, {
    label: 'Demo Echo',
    description: 'Local test operation',
    requestSchema: requestContract.requestSchema('POST', '')
  })

  await gateway.start()
  console.log(`Gateway is running at ws://127.0.0.1:${config.server.port}/ws`)
  console.log('Configure Gateway Call with operation demo/echo')

  const shutdown = async () => {
    await gateway.stop()
    process.exit(0)
  }
  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
}

main().catch(error => {
  console.error(error.stack || error.message)
  process.exitCode = 1
})
