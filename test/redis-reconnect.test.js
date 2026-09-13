'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { EventEmitter } = require('node:events')
const { createReconnectStrategy, attachErrorLogger } = require('../lib/redis-reconnect')

test('before the client has ever connected, the strategy fails fast like a normal Redis client', () => {
  const strategy = createReconnectStrategy({ maxInitialRetries: 2 })
  assert.equal(strategy(0), 0)
  assert.equal(strategy(1), 200)
  assert.equal(strategy(2), 400)
  assert.ok(strategy(3) instanceof Error, 'must give up while never-ready, so startup callers fail fast (e.g. t.skip in integration tests)')
})

test('after markReady(), the strategy never gives up no matter how many retries', () => {
  const strategy = createReconnectStrategy({ stepMs: 200, maxDelayMs: 1000 })
  strategy.markReady()
  for (const retries of [0, 1, 2, 3, 10, 100, 100000]) {
    const delay = strategy(retries)
    assert.equal(typeof delay, 'number', `retries=${retries} must return a number once the client has connected, not give up`)
    assert.ok(Number.isFinite(delay) && delay >= 0)
  }
})

test('post-ready backoff is linear then capped at maxDelayMs', () => {
  const strategy = createReconnectStrategy({ stepMs: 200, maxDelayMs: 1000 })
  strategy.markReady()
  assert.equal(strategy(0), 0)
  assert.equal(strategy(1), 200)
  assert.equal(strategy(2), 400)
  assert.equal(strategy(10), 1000)
  assert.equal(strategy(100000), 1000)
})

test('attachErrorLogger flips the paired strategy to unbounded-retry mode on first ready', () => {
  const client = new EventEmitter()
  const strategy = createReconnectStrategy({ maxInitialRetries: 2 })
  attachErrorLogger(client, { error () {}, info () {} }, 'redis test', { strategy })

  assert.ok(strategy(3) instanceof Error, 'still fails fast before the first ready event')
  client.emit('ready')
  assert.equal(typeof strategy(3), 'number', 'must retry forever once the client has connected at least once')
})

test('attachErrorLogger logs the first error immediately and throttles repeats', () => {
  const client = new EventEmitter()
  const messages = []
  const logger = { error: (...args) => messages.push(['error', args.join(' ')]), info: (...args) => messages.push(['info', args.join(' ')]) }
  attachErrorLogger(client, logger, 'redis test', { throttleMs: 10000 })

  client.emit('error', new Error('first blip'))
  client.emit('error', new Error('second blip, same outage'))
  assert.equal(messages.filter(([level]) => level === 'error').length, 1, 'second error within throttle window must not log again')

  client.emit('ready')
  assert.equal(messages.at(-1)[0], 'info')
  assert.match(messages.at(-1)[1], /connection restored/)
})

test('attachErrorLogger does not log a recovery line if the client was never down', () => {
  const client = new EventEmitter()
  const messages = []
  const logger = { error: (...args) => messages.push(args), info: (...args) => messages.push(args) }
  attachErrorLogger(client, logger, 'redis test')

  client.emit('ready')
  assert.equal(messages.length, 0)
})
