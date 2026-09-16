'use strict'

const { parseOperation } = require('./protocol')

function getClient (RED, configId, node) {
  const config = RED.nodes.getNode(configId)
  if (!config || !config.client) {
    node.error('Gateway Config is not configured')
    return null
  }
  return config
}

function setConnectionStatus (node, state) {
  const status = {
    connecting: { fill: 'yellow', shape: 'ring', text: 'connecting' },
    connected: { fill: 'green', shape: 'dot', text: 'connected' },
    reconnecting: { fill: 'yellow', shape: 'ring', text: 'reconnecting' },
    disconnected: { fill: 'red', shape: 'ring', text: 'disconnected' },
    closed: { fill: 'grey', shape: 'ring', text: 'closed' }
  }
  node.status(status[state] || status.disconnected)
}

function operationFromNode (node, msg) {
  const value = node.operation || msg?.gateway?.operation
  return parseOperation(value)
}

function gatewayMeta (requestId, operation, status, extra = {}) {
  return {
    requestId,
    service: operation.service,
    operation: operation.operation,
    operationKey: operation.value,
    status,
    ...extra
  }
}

function errorMessage (RED, msg, error, meta) {
  const output = RED.util.cloneMessage(msg)
  output.error = {
    code: error.code || 'GATEWAY_ERROR',
    message: error.message || 'Gateway request failed',
    retryable: Boolean(error.retryable),
    ...(error.retryAfterMs != null ? { retryAfterMs: Number(error.retryAfterMs) } : {}),
    ...(Array.isArray(error.details) ? { details: error.details } : {})
  }
  output.gateway = { ...(output.gateway || {}), ...meta, status: 'failed' }
  return output
}

/**
 * A read-only view of this node's Node-RED global context, shaped so
 * lib/url-template.js can address it with plain dot-path template
 * expressions like {{global.apiToken}}. Node-RED's global context is
 * accessed through .get(name), not plain property reads, so the first
 * segment of the path is proxied into a .get() call; anything the call
 * returns (a string, or a nested object for {{global.config.token}}) is
 * then indexed normally.
 *
 * Returns a static {} (every read resolves to undefined) if the node has
 * no context -- e.g. in unit tests that construct nodes without the full
 * Node-RED runtime -- so callers never need a null check.
 */
function globalContextAccessor (node) {
  const globalContext = typeof node?.context === 'function' ? node.context().global : null
  if (!globalContext || typeof globalContext.get !== 'function') return {}
  return new Proxy({}, {
    get (_target, prop) {
      if (typeof prop !== 'string') return undefined
      return globalContext.get(prop)
    },
    has () {
      return true
    }
  })
}

function closeListener (node, client, event, listener) {
  node.on('close', (_removed, done) => {
    client.removeListener(event, listener)
    if (typeof done === 'function') done()
  })
}

module.exports = {
  getClient,
  setConnectionStatus,
  operationFromNode,
  gatewayMeta,
  errorMessage,
  closeListener,
  globalContextAccessor
}
