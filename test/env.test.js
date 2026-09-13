'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')
const { loadDotEnv } = require('../lib/env')

test('loads dotenv values, comments and quoted strings without overwriting process env', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'pod-gateway-env-'))
  const file = path.join(directory, '.env')
  const names = ['POD_GATEWAY_TEST_ALPHA', 'POD_GATEWAY_TEST_BETA', 'POD_GATEWAY_TEST_EXISTING']
  fs.writeFileSync(file, [
    '# comment',
    'POD_GATEWAY_TEST_ALPHA=one',
    'POD_GATEWAY_TEST_BETA="two words"',
    'POD_GATEWAY_TEST_EXISTING=from-file',
    'not valid',
    ''
  ].join('\n'))
  const previous = Object.fromEntries(names.map(name => [name, process.env[name]]))
  process.env.POD_GATEWAY_TEST_EXISTING = 'from-process'
  try {
    assert.equal(loadDotEnv(file), true)
    assert.equal(process.env.POD_GATEWAY_TEST_ALPHA, 'one')
    assert.equal(process.env.POD_GATEWAY_TEST_BETA, 'two words')
    assert.equal(process.env.POD_GATEWAY_TEST_EXISTING, 'from-process')
  } finally {
    for (const name of names) {
      if (previous[name] === undefined) delete process.env[name]
      else process.env[name] = previous[name]
    }
    fs.rmSync(directory, { recursive: true, force: true })
  }
})

test('returns false when the dotenv file does not exist', () => {
  assert.equal(loadDotEnv('/tmp/pod-gateway-file-that-does-not-exist'), false)
})
