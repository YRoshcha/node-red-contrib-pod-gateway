'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { GatewayServer } = require('../lib/gateway-server')
const { GatewayClient } = require('../lib/gateway-client')
const { InMemoryGcraRateLimiter } = require('../lib/in-memory-rate-limiter')
const { requestSchema } = require('../lib/request-contract')

async function createGateway (options = {}) {
  let executions = 0
  const server = new GatewayServer({
    port: 0,
    rateLimiter: options.rateLimiter,
    concurrency: options.concurrency || 4,
    adapters: {
      'demo/echo': async payload => {
        executions++
        return { echoed: payload }
      }
    }
  })
  await server.start()
  const port = server.httpServer.address().port
  return { server, url: `ws://127.0.0.1:${port}/ws`, executions: () => executions }
}

class FakeRedisQueue {
  constructor () {
    this.items = []
    this.handler = null
    this.resultHandler = null
    this.id = 0
    this.instanceId = 'fake-queue'
  }

  async start (handler, resultHandler) {
    this.handler = handler
    this.resultHandler = resultHandler
  }

  async enqueue (item) {
    const id = `1-0-${++this.id}`
    this.items.push({ ...item, queueId: id })
    setImmediate(async () => {
      const next = this.items.shift()
      if (next) await this.handler(next, { id, stream: 'fake' })
    })
    return { id, stream: 'fake' }
  }

  async storeIdempotencyResult () {}
  async publishResult (_origin, podId, instanceId, response) {
    await this.resultHandler({ podId, instanceId, response })
  }
  async stop () {}
}

test('Gateway Call goes POD -> gateway -> adapter -> same POD', async t => {
  const gateway = await createGateway()
  t.after(async () => gateway.server.stop())
  const client = new GatewayClient({ url: gateway.url, podId: 'pod-1', reconnectInterval: 50 })
  t.after(() => client.close())

  const result = await client.request({
    type: 'call',
    operation: 'demo/echo',
    payload: { value: 42 },
    deadlineAt: new Date(Date.now() + 5000).toISOString()
  }, 5000)

  assert.deepEqual(result.payload, { echoed: { value: 42 } })
  assert.equal(gateway.executions(), 1)
})

test('request contract crosses the WebSocket and returns the upstream HTTP status', async t => {
  const server = new GatewayServer({
    port: 0,
    adapters: {
      'rider/update': async (payload, context) => {
        assert.deepEqual(payload, { id: 'rider-42' })
        assert.equal(context.method, 'PUT')
        assert.deepEqual(context.request.body, { active: false })
        assert.deepEqual(context.request.headers, { 'X-Trace': '{{gateway.requestId}}' })
        context.response = { statusCode: 200 }
        return { updated: true }
      }
    }
  })
  server.registerOperation('rider/update', server.adapters.get('rider/update'), {
    requestSchema: requestSchema('PUT', '/v1/riders/{{request.params.id}}')
  })
  await server.start()
  t.after(async () => server.stop())
  const client = new GatewayClient({ url: `ws://127.0.0.1:${server.httpServer.address().port}/ws`, podId: 'pod-contract', reconnectInterval: 50 })
  t.after(() => client.close())
  const result = await client.request({
    type: 'call',
    operation: 'rider/update',
    payload: { id: 'rider-42' },
    _request: {
      params: { id: 'rider-42' },
      headers: { 'X-Trace': '{{gateway.requestId}}' },
      body: { active: false }
    },
    deadlineAt: new Date(Date.now() + 5000).toISOString()
  }, 5000)
  assert.equal(result.response.statusCode, 200)
  assert.equal(result._request.input.method, 'PUT')
  assert.equal(result._request.output.statusCode, 200)
  assert.deepEqual(result._request.output.body, { updated: true })
})

test('Gateway Out receives its async result as a message', async t => {
  const gateway = await createGateway()
  t.after(async () => gateway.server.stop())
  const client = new GatewayClient({ url: gateway.url, podId: 'pod-2', reconnectInterval: 50 })
  t.after(() => client.close())
  const messagePromise = new Promise(resolve => client.once('message', resolve))
  await client.send({ type: 'event', operation: 'demo/echo', payload: 'async' }, 5000)
  const message = await messagePromise
  assert.equal(message.type, 'result')
  assert.equal(message.operation, 'demo/echo')
  assert.deepEqual(message.payload, { echoed: 'async' })
})

test('all connected PODs share the gateway rate limiter', async t => {
  const gateway = await createGateway({
    rateLimiter: new InMemoryGcraRateLimiter({ demo: { rate: 20, burst: 1 } })
  })
  t.after(async () => gateway.server.stop())
  const clients = [1, 2].map(index => new GatewayClient({
    url: gateway.url,
    podId: `pod-${index}`,
    reconnectInterval: 50
  }))
  t.after(() => clients.forEach(client => client.close()))
  const started = Date.now()
  await Promise.all(clients.map(client => client.request({
    type: 'call',
    operation: 'demo/echo',
    payload: 'shared',
    deadlineAt: new Date(Date.now() + 5000).toISOString()
  }, 5000)))
  assert.ok(Date.now() - started >= 30)
  assert.equal(gateway.executions(), 2)
})

test('passes the full operation key to the gateway rate limiter', async t => {
  const calls = []
  const gateway = await createGateway({
    rateLimiter: {
      allow: async (...args) => {
        calls.push(args)
        return { allowed: true, retryAfterMs: 0 }
      }
    }
  })
  t.after(async () => gateway.server.stop())
  const client = new GatewayClient({ url: gateway.url, podId: 'pod-operation-limit', reconnectInterval: 50 })
  t.after(() => client.close())

  await client.request({
    type: 'call',
    operation: 'demo/echo',
    payload: 'operation-aware',
    deadlineAt: new Date(Date.now() + 5000).toISOString()
  }, 5000)

  assert.deepEqual(calls[0].slice(0, 3), ['demo', 1, 'demo/echo'])
})

test('GatewayServer can process a shared queue abstraction', async t => {
  const queue = new FakeRedisQueue()
  const gateway = new GatewayServer({
    port: 0,
    queue,
    adapters: { 'demo/echo': async payload => ({ echoed: payload }) }
  })
  await gateway.start()
  t.after(async () => gateway.stop())
  const client = new GatewayClient({ url: `ws://127.0.0.1:${gateway.httpServer.address().port}/ws`, podId: 'pod-queue' })
  t.after(() => client.close())
  const result = await client.request({ type: 'call', operation: 'demo/echo', payload: 'redis-shaped' }, 5000)
  assert.deepEqual(result.payload, { echoed: 'redis-shaped' })
})

test('shared queue path applies the global limiter before the adapter', async t => {
  let decisions = 0
  const queue = new FakeRedisQueue()
  const gateway = new GatewayServer({
    port: 0,
    queue,
    rateLimiter: {
      allow: async () => {
        decisions++
        return decisions === 1 ? { allowed: false, retryAfterMs: 2 } : { allowed: true, retryAfterMs: 0 }
      }
    },
    adapters: { 'demo/echo': async payload => ({ echoed: payload }) }
  })
  await gateway.start()
  t.after(async () => gateway.stop())
  const client = new GatewayClient({ url: `ws://127.0.0.1:${gateway.httpServer.address().port}/ws`, podId: 'pod-queue-rate' })
  t.after(() => client.close())
  const result = await client.request({ type: 'call', operation: 'demo/echo', payload: 'limited' }, 5000)
  assert.deepEqual(result.payload, { echoed: 'limited' })
  assert.equal(decisions, 2)
})

test('idempotency prevents duplicate upstream execution', async t => {
  let executions = 0
  const server = new GatewayServer({
    port: 0,
    adapters: {
      'demo/slow': async payload => {
        executions++
        await new Promise(resolve => setTimeout(resolve, 40))
        return { echoed: payload }
      }
    }
  })
  await server.start()
  t.after(async () => server.stop())
  const client = new GatewayClient({ url: `ws://127.0.0.1:${server.httpServer.address().port}/ws`, podId: 'pod-idem' })
  t.after(() => client.close())
  const first = client.request({ type: 'call', operation: 'demo/slow', payload: 'once', idempotencyKey: 'same-key' }, 5000)
  await new Promise(resolve => setTimeout(resolve, 5))
  const second = client.request({ type: 'call', operation: 'demo/slow', payload: 'once', idempotencyKey: 'same-key' }, 5000)
  const results = await Promise.all([first, second])
  assert.deepEqual(results[0].payload, { echoed: 'once' })
  assert.deepEqual(results[1].payload, { echoed: 'once' })
  assert.equal(executions, 1)
})

test('GatewayClient fails fast when a call is not accepted', async () => {
  const client = new GatewayClient({ url: 'ws://unused' })
  client.connect = async () => {}
  client.ws = { readyState: 1 }
  client._sendRaw = () => {}

  await assert.rejects(
    client.request({ type: 'call', operation: 'demo/echo', payload: 'timeout' }, 100, { acceptTimeoutMs: 10 }),
    error => error.code === 'GATEWAY_ACCEPT_TIMEOUT'
  )
})

test('a successful Redis idempotency claim continues to enqueue', async () => {
  const server = new GatewayServer({
    queue: {
      claimIdempotency: async () => ({ claimed: true })
    }
  })

  assert.equal(await server._claimIdempotency('pod-1', 'key-1', 'request-1', 'pod-1:key-1'), null)
})
