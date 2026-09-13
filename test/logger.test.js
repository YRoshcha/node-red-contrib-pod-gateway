'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { createLogger } = require('../lib/logger')

test('verbose logger routes trace messages to the normal log channel', () => {
  const entries = []
  const logger = createLogger({
    info: (...args) => entries.push(args),
    warn: () => {},
    error: () => {}
  }, { verbose: true })

  logger.debug('request', 'abc')

  assert.deepEqual(entries, [['[pod-gateway][trace] request abc']])
})

test('non-verbose logger suppresses trace messages', () => {
  let called = false
  const logger = createLogger({ info: () => { called = true } }, { verbose: false })

  logger.debug('hidden')

  assert.equal(called, false)
})
