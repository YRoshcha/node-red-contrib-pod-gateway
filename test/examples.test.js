'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const test = require('node:test')
const { parseOperation } = require('../lib/protocol')

const examplesDirectory = path.join(__dirname, '..', 'examples')

for (const file of fs.readdirSync(examplesDirectory).filter(name => name.endsWith('.json')).sort()) {
  test(`Node-RED example ${file} is a valid importable flow`, () => {
    const flow = JSON.parse(fs.readFileSync(path.join(examplesDirectory, file), 'utf8'))
    assert.ok(Array.isArray(flow), 'Node-RED import files must contain an array')
    assert.ok(flow.length > 0)
    const ids = new Set()
    const tabs = new Set(flow.filter(node => node.type === 'tab').map(node => node.id))
    assert.ok(tabs.size > 0, 'flow must contain a tab')
    for (const node of flow) {
      assert.equal(typeof node.id, 'string')
      assert.equal(ids.has(node.id), false, `duplicate id ${node.id}`)
      ids.add(node.id)
      if (node.z) assert.equal(tabs.has(node.z), true, `${node.id} references a missing tab`)
      if (['gateway-call', 'gateway-out', 'gateway-in'].includes(node.type)) {
        assert.equal(typeof node.gateway, 'string')
        assert.equal(flow.some(candidate => candidate.id === node.gateway && candidate.type === 'gateway-config'), true)
        parseOperation(node.operation || node.event)
      }
      if (node.type === 'gateway-adapter') {
        assert.equal(flow.some(candidate => candidate.id === node.server && candidate.type === 'gateway-server-config'), true)
        assert.equal(flow.some(candidate => candidate.id === node.api && candidate.type === 'gateway-api-config'), true)
        parseOperation(node.operation)
      }
    }
    for (const node of flow) {
      for (const wireGroup of node.wires || []) {
        for (const target of wireGroup) assert.equal(ids.has(target), true, `${node.id} wires to missing node ${target}`)
      }
    }
  })
}

test('custom headers example demonstrates dynamic GET and POST headers', () => {
  const flow = JSON.parse(fs.readFileSync(path.join(examplesDirectory, 'custom-headers-flow.json'), 'utf8'))
  const adapters = flow.filter(node => node.type === 'gateway-adapter')
  assert.deepEqual(adapters.map(node => [node.operation, node.method]).sort(), [
    ['demo/get', 'GET'],
    ['demo/post', 'POST']
  ])
  const requestFunctions = flow.filter(node => node.type === 'function' && node.name.startsWith('Set '))
  assert.equal(requestFunctions.length, 2)
  for (const node of requestFunctions) {
    assert.match(node.func, /msg\._request\s*=\s*\{/)
    assert.match(node.func, /X-Request-ID/)
  }
})
