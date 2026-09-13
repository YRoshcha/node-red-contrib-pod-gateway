'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const test = require('node:test')

const nodesDirectory = path.join(__dirname, '..', 'nodes')

test('Gateway Adapter exposes an explicit HTTP method dropdown', () => {
  const html = fs.readFileSync(path.join(nodesDirectory, 'gateway-adapter.html'), 'utf8')
  assert.match(html, /<select id="node-input-method">/)
  for (const method of ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD']) {
    assert.match(html, new RegExp(`<option value="${method}">${method}</option>`))
  }
})

for (const file of ['gateway-call.html', 'gateway-out.html']) {
  test(`${file} exposes an available-operation picker with manual fallback`, () => {
    const html = fs.readFileSync(path.join(nodesDirectory, file), 'utf8')
    assert.match(html, /id="node-input-operation"/)
    assert.match(html, /id="pod-gateway-operation-picker"/)
    assert.match(html, /enter .*manually|enter a new/i)
    assert.match(html, /pod-gateway\/(?:capabilities|server-capabilities)\//)
  })
}

test('Gateway Adapter keeps new operation entry manual and separates the HTTP method dropdown', () => {
  const html = fs.readFileSync(path.join(nodesDirectory, 'gateway-adapter.html'), 'utf8')
  assert.match(html, /id="node-input-name"/)
  assert.match(html, /<input type="text" id="node-input-operation" placeholder="service\/operation">/)
  assert.doesNotMatch(html, /operation-picker/)
  assert.match(html, /<select id="node-input-method">/)
})

test('Gateway Call shows the selected adapter contract', () => {
  const html = fs.readFileSync(path.join(nodesDirectory, 'gateway-call.html'), 'utf8')
  assert.match(html, /id="node-input-name"/)
  assert.match(html, /id="pod-gateway-contract"/)
  assert.match(html, /Adapter contract/)
  assert.match(html, /Accepted request fields/)
  assert.match(html, /pod-gateway\/capabilities\//)
  assert.doesNotMatch(html, /operation-manual-toggle/)
  assert.doesNotMatch(html, /operationPicker\.on\('focus'/)
})

test('Gateway In exposes an available-operation picker while retaining the all-events option', () => {
  const html = fs.readFileSync(path.join(nodesDirectory, 'gateway-in.html'), 'utf8')
  assert.match(html, /id="node-input-event"/)
  assert.match(html, /id="pod-gateway-event-picker"/)
  assert.match(html, /receive all events/i)
  assert.match(html, /pod-gateway\/capabilities\//)
})
