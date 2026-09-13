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
  closeListener
}
