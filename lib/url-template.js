'use strict'

const TEMPLATE_RE = /{{\s*([^{}]+?)\s*}}/g
const TEMPLATE_DETECT_RE = /{{\s*[^{}]+?\s*}}/

function hasUrlTemplate (value) {
  return TEMPLATE_DETECT_RE.test(String(value || ''))
}

function resolveUrlTemplate (template, payload, context = {}) {
  return resolveTemplate(template, payload, context, {
    errorCode: 'INVALID_URL_TEMPLATE',
    encode: true
  })
}

/**
 * Resolve a scalar template without URL encoding. This is used for dynamic
 * HTTP header values, where encoding would change the value sent upstream.
 */
function resolveHeaderTemplate (template, payload, context = {}) {
  return resolveTemplate(template, payload, context, {
    errorCode: 'INVALID_HEADER_TEMPLATE',
    encode: false
  })
}

function resolveTemplate (template, payload, context = {}, options = {}) {
  const source = String(template || '')
  const encode = options.encode !== false
  const errorCode = options.errorCode || 'INVALID_URL_TEMPLATE'
  const label = errorCode === 'INVALID_HEADER_TEMPLATE' ? 'Header' : 'URL'
  return source.replace(TEMPLATE_RE, (_match, expression) => {
    const value = readValue(String(expression).trim(), payload, context, errorCode, label)
    if (value === undefined || value === null) {
      throw templateError(`${label} template variable "${String(expression).trim()}" is missing`, errorCode)
    }
    if (typeof value === 'object' || typeof value === 'function') {
      throw templateError(`${label} template variable "${String(expression).trim()}" must be a scalar value`, errorCode)
    }
    const resolved = String(value)
    if (errorCode === 'INVALID_HEADER_TEMPLATE' && /[\r\n]/.test(resolved)) {
      throw templateError(`Header template variable "${String(expression).trim()}" contains an invalid line break`, errorCode)
    }
    return encode ? encodeURIComponent(resolved) : resolved
  })
}

function readValue (expression, payload, context, errorCode = 'INVALID_URL_TEMPLATE', label = 'URL') {
  const segments = parsePath(expression, errorCode, label)
  const root = segments.shift()
  const request = context?.request || {}
  const roots = {
    payload,
    request,
    _request: request,
    msg: { payload, _request: request },
    context,
    gateway: context
  }
  if (!Object.prototype.hasOwnProperty.call(roots, root)) {
    throw templateError(`${label} template root "${root}" is not supported; use payload, request, msg.payload, or gateway`, errorCode)
  }

  let value = roots[root]
  for (const segment of segments) {
    if (isUnsafeKey(segment)) throw templateError(`${label} template property "${segment}" is not allowed`, errorCode)
    if (value === undefined || value === null) return undefined
    value = Object(value)[segment]
  }
  return value
}

function parsePath (expression, errorCode = 'INVALID_URL_TEMPLATE', label = 'URL') {
  const normalized = expression.replace(/\[(['"]?)([^'"\]]+)\1\]/g, '.$2')
  const segments = normalized.split('.').map(value => value.trim()).filter(Boolean)
  if (!segments.length || segments.some(segment => !(/^[A-Za-z_$][A-Za-z0-9_$-]*$/.test(segment) || /^\d+$/.test(segment)))) {
    throw templateError(`${label} template path "${expression}" is invalid`, errorCode)
  }
  return segments
}

function isUnsafeKey (key) {
  return key === '__proto__' || key === 'prototype' || key === 'constructor'
}

function templateError (message, code = 'INVALID_URL_TEMPLATE') {
  const error = new Error(message)
  error.code = code
  error.retryable = false
  return error
}

module.exports = { hasUrlTemplate, resolveHeaderTemplate, resolveUrlTemplate }
