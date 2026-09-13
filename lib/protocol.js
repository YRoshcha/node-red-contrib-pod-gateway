'use strict'

const { randomUUID } = require('node:crypto')

const PROTOCOL_VERSION = '1.0'

const PRIORITY = Object.freeze({
  normal: 1,
  bulk: 0
})

function makeRequestId () {
  return typeof randomUUID === 'function'
    ? randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
}

function parseOperation (operation) {
  if (typeof operation !== 'string' || !operation.trim()) {
    throw new Error('operation must be a non-empty string')
  }
  const value = operation.trim()
  const slash = value.indexOf('/')
  if (slash <= 0 || slash === value.length - 1) {
    throw new Error('operation must use the form service/name')
  }
  const service = value.slice(0, slash)
  const operationName = value.slice(slash + 1)
  if (!/^[A-Za-z0-9._-]+$/.test(service) || !/^[A-Za-z0-9._:-]+$/.test(operationName)) {
    throw new Error('operation contains unsupported characters')
  }
  return {
    service,
    operation: operationName,
    value
  }
}

function validateEnvelope (message) {
  if (!message || typeof message !== 'object') {
    throw new Error('message must be an object')
  }
  if (typeof message.type !== 'string') {
    throw new Error('message.type is required')
  }
  if (['call', 'event'].includes(message.type)) {
    if (typeof message.requestId !== 'string' || !message.requestId) {
      throw new Error('requestId is required')
    }
    parseOperation(message.operation)
  }
  return message
}

function errorPayload (error, defaults = {}) {
  return {
    code: error.code || defaults.code || 'GATEWAY_ERROR',
    message: error.message || defaults.message || 'Gateway request failed',
    retryable: Boolean(error.retryable ?? defaults.retryable),
    ...(error.retryAfterMs != null || defaults.retryAfterMs != null
      ? { retryAfterMs: Number(error.retryAfterMs ?? defaults.retryAfterMs) }
      : {}),
    ...(Array.isArray(error.details) ? { details: error.details } : {})
  }
}

module.exports = {
  PROTOCOL_VERSION,
  PRIORITY,
  makeRequestId,
  parseOperation,
  validateEnvelope,
  errorPayload
}
