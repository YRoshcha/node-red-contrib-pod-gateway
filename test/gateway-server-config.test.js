'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { EventEmitter } = require('node:events')
const { RedisStreamQueue } = require('../lib/redis-stream-queue')
const { RedisGcraRateLimiter } = require('../lib/redis-rate-limiter')
const registerGatewayServerConfig = require('../nodes/gateway-server-config')

function makeRED () {
  const registered = {}
  const RED = {
    nodes: {
      createNode: (node, config) => {
        EventEmitter.call(node)
        Object.setPrototypeOf(node, EventEmitter.prototype)
        node.id = config.id || 'test-node'
        node.credentials = config.credentials || {}
        node.status = () => {}
        node.error = () => {}
      },
      registerType: (name, constructor) => { registered[name] = constructor }
    },
    log: { info () {}, warn () {}, error () {} }
  }
  return { RED, registered }
}

// gateway-server-config.js talks to real Redis via RedisStreamQueue.connect /
// RedisGcraRateLimiter.connect. Both are the same class objects everywhere
// (Node's module cache), so swapping their static `connect` for the duration
// of a test intercepts the node's own calls without touching real Redis.
function stubRedis (fakeQueue = {}) {
  const originalQueueConnect = RedisStreamQueue.connect
  const originalLimiterConnect = RedisGcraRateLimiter.connect
  let capturedQueueOptions = null
  RedisStreamQueue.connect = async options => {
    capturedQueueOptions = options
    return {
      claimIdleMs: options.claimIdleMs,
      start: async () => {},
      stop: async () => {},
      on: () => {},
      ...fakeQueue
    }
  }
  RedisGcraRateLimiter.connect = async () => ({ close: async () => {} })
  return {
    getCapturedQueueOptions: () => capturedQueueOptions,
    restore: () => {
      RedisStreamQueue.connect = originalQueueConnect
      RedisGcraRateLimiter.connect = originalLimiterConnect
    }
  }
}

test('derives a claimIdleMs floor from the configured upstreamTimeoutMs (default margin)', async t => {
  const stub = stubRedis()
  t.after(stub.restore)
  const { RED, registered } = makeRED()
  registerGatewayServerConfig(RED)
  const GatewayServerConfigNode = registered['gateway-server-config']

  const node = new GatewayServerConfigNode({ id: 'srv-default', port: 0 })
  t.after(() => node.server?.stop())
  await node.ready

  // Default upstreamTimeoutMs is 60000, well under the 120000 hardcoded
  // default -- claimIdleMs must stay at that default, not shrink.
  assert.equal(stub.getCapturedQueueOptions().claimIdleMs, 120000)
})

test('raises claimIdleMs above a configured upstreamTimeoutMs that would otherwise exceed it', async t => {
  const stub = stubRedis()
  t.after(stub.restore)
  const { RED, registered } = makeRED()
  registerGatewayServerConfig(RED)
  const GatewayServerConfigNode = registered['gateway-server-config']

  const node = new GatewayServerConfigNode({ id: 'srv-slow', port: 0, upstreamTimeoutMs: 150000 })
  t.after(() => node.server?.stop())
  await node.ready

  assert.equal(stub.getCapturedQueueOptions().claimIdleMs, 150000 + 30000)
})
