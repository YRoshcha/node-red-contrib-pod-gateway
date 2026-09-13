'use strict'

const assert = require('node:assert/strict')
const test = require('node:test')
const { normalizeRedisUrl } = require('../lib/redis-url')

test('normalizes a Redis host and port', () => {
  assert.equal(normalizeRedisUrl('localhost:6379'), 'redis://localhost:6379')
})

test('accepts Redis and Redis TLS URLs', () => {
  assert.equal(normalizeRedisUrl('redis://redis:6379'), 'redis://redis:6379')
  assert.equal(normalizeRedisUrl('rediss://redis.example.com:6380'), 'rediss://redis.example.com:6380')
})

test('rejects non-Redis protocols', () => {
  assert.throws(() => normalizeRedisUrl('http://localhost:6379'), /redis:\/\/ or rediss:\/\//)
})

test('adds the default scheme and preserves credentials, database and TLS', () => {
  assert.equal(normalizeRedisUrl(' redis.example.com:6380 '), 'redis://redis.example.com:6380')
  assert.equal(normalizeRedisUrl('rediss://user:pass@redis.example.com:6380/2'), 'rediss://user:pass@redis.example.com:6380/2')
  assert.equal(normalizeRedisUrl(''), 'redis://127.0.0.1:6379')
})
