'use strict'

const { hasUrlTemplate, resolveHeaderTemplate, resolveUrlTemplate } = require('./url-template')

const HTTP_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'])
const BODY_METHODS = new Set(['POST', 'PUT', 'PATCH', 'DELETE'])
const REQUEST_KEYS = new Set(['params', 'query', 'headers', 'body', 'method', 'payload'])
const PROTECTED_HEADERS = new Set([
  'authorization',
  'cookie',
  'host',
  'content-length',
  'proxy-authorization',
  'x-api-key',
  'x-auth-token',
  'x-rapidapi-key',
  'api-key'
])

// PROTECTED_HEADERS only covers the common credential header names. A
// Gateway API Config can put its API key behind any header name the
// operator chooses (`apiKeyHeader`); if that name is not on the static
// list, a POD's own _request.headers would otherwise be free to send a
// header with the exact same name and silently override the real
// credential in the upstream request (object spread in http-adapter.js
// applies the POD-supplied headers last). Callers that know a specific
// adapter's configured credential header name(s) pass them here so they
// are protected too, in addition to the static list.
function protectedHeaderSet (extra) {
  if (!extra || !extra.length) return PROTECTED_HEADERS
  const merged = new Set(PROTECTED_HEADERS)
  for (const name of extra) {
    if (name) merged.add(String(name).toLowerCase())
  }
  return merged
}

function isPlainObject (value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  // Do not compare prototypes with strict ===. Objects built inside a
  // Node-RED Function node run in a separate vm context/realm, so a literal
  // `{}` there has a *different* Object.prototype reference even though it
  // is structurally a plain object. Object.prototype.toString.call is
  // realm-independent (it inspects the internal [[Class]] slot, not the
  // prototype chain identity) and is the standard way to detect this
  // correctly across realms.
  return Object.prototype.toString.call(value) === '[object Object]'
}

function requestSource (value) {
  if (value === undefined) return { source: {}, envelope: false }
  if (!isPlainObject(value)) return { source: {}, envelope: false, error: '_request must be an object' }
  if (Object.prototype.hasOwnProperty.call(value, 'input')) {
    if (!isPlainObject(value.input)) return { source: {}, envelope: true, error: '_request.input must be an object' }
    return { source: value.input, envelope: true }
  }
  return { source: value, envelope: false }
}

/**
 * Canonical transport options carried by msg._request. The object deliberately
 * contains only request data that a POD is allowed to provide; API credentials
 * remain in the central Gateway API Config.
 */
function normalizeRequest (value) {
  const source = requestSource(value).source
  const request = {}
  for (const key of ['params', 'query', 'headers']) {
    if (source[key] !== undefined) request[key] = source[key]
  }
  if (Object.prototype.hasOwnProperty.call(source, 'body')) request.body = source.body
  return request
}

function requestInput (payload, request, method) {
  return {
    ...(method ? { method } : {}),
    ...normalizeRequest(request),
    payload
  }
}

function requestEnvelope (payload, request, method, response = {}) {
  return {
    input: requestInput(payload, request, method),
    output: {
      ...(response.status ? { status: response.status } : {}),
      ...(Object.prototype.hasOwnProperty.call(response, 'statusCode')
        ? { statusCode: response.statusCode == null ? null : Number(response.statusCode) }
        : {}),
      ...(Object.prototype.hasOwnProperty.call(response, 'body') ? { body: response.body } : {}),
      ...(response.error ? { error: response.error } : {})
    }
  }
}

function methodUsesBody (method) {
  return BODY_METHODS.has(String(method || '').toUpperCase())
}

function requestSchema (method, template) {
  const normalizedMethod = String(method || 'POST').toUpperCase()
  if (!HTTP_METHODS.has(normalizedMethod)) {
    throw new Error(`Unsupported HTTP method ${normalizedMethod}; use GET, POST, PUT, PATCH, DELETE or HEAD`)
  }
  return {
    method: normalizedMethod,
    template: String(template || ''),
    body: methodUsesBody(normalizedMethod),
    query: true,
    params: true,
    headers: true
  }
}

function validateRequest (payload, requestValue, schema, context = {}) {
  const sourceInfo = requestSource(requestValue)
  const request = normalizeRequest(requestValue)
  const errors = []

  if (sourceInfo.error) errors.push(sourceInfo.error)
  if (!sourceInfo.error) {
    for (const key of Object.keys(sourceInfo.source)) {
      // `payload` is included only in the output envelope so that a result can
      // be routed back through another Gateway Call without manual reshaping.
      if (!REQUEST_KEYS.has(key) || (key === 'payload' && !sourceInfo.envelope)) {
        errors.push(`_request.${key} is not supported; use params, query, headers or body`)
      }
    }
  }

  const requestedMethod = sourceInfo.source.method
  if (requestedMethod !== undefined) {
    if (typeof requestedMethod !== 'string') errors.push('_request.method must be a string')
    else {
      const normalizedRequestedMethod = String(requestedMethod).toUpperCase()
      if (!HTTP_METHODS.has(normalizedRequestedMethod)) {
        errors.push(`Unsupported HTTP method ${normalizedRequestedMethod}; use GET, POST, PUT, PATCH, DELETE or HEAD`)
      } else if (schema?.method && normalizedRequestedMethod !== String(schema.method).toUpperCase()) {
        errors.push(`_request.method must be ${schema.method}`)
      }
    }
  }

  for (const [name, value] of [['params', request.params], ['query', request.query], ['headers', request.headers]]) {
    if (value !== undefined && !isPlainObject(value)) errors.push(`_request.${name} must be an object`)
  }
  if (isPlainObject(request.params)) {
    for (const [key, value] of Object.entries(request.params)) {
      if (value !== null && typeof value === 'object') errors.push(`_request.params.${key} must be a scalar value`)
    }
  }
  if (isPlainObject(request.query)) {
    for (const [key, value] of Object.entries(request.query)) {
      const values = Array.isArray(value) ? value : [value]
      if (values.some(item => item !== null && item !== undefined && typeof item === 'object')) {
        errors.push(`_request.query.${key} must contain scalar values`)
      }
    }
  }
  const protectedHeaders = protectedHeaderSet(schema?.protectedHeaders)
  if (isPlainObject(request.headers)) {
    for (const key of Object.keys(request.headers)) {
      if (protectedHeaders.has(key.toLowerCase())) errors.push(`_request.headers.${key} is managed by the Gateway API Config`)
      const value = request.headers[key]
      if (value !== null && typeof value === 'object') errors.push(`_request.headers.${key} must be a scalar value`)
      if (typeof value === 'string') {
        try {
          const resolved = hasUrlTemplate(value)
            ? resolveHeaderTemplate(value, payload, { ...context, request })
            : value
          if (/[\r\n]/.test(resolved)) {
            errors.push(`_request.headers.${key} contains an invalid line break`)
          }
        } catch (error) {
          errors.push(error.message)
        }
      }
    }
  }
  if (schema && !schema.body && Object.prototype.hasOwnProperty.call(request, 'body') && request.body !== undefined) {
    errors.push(`${schema.method} requests must use _request.query instead of _request.body`)
  }

  if (schema?.template && hasUrlTemplate(schema.template)) {
    try {
      resolveUrlTemplate(schema.template, payload, { ...context, request })
    } catch (error) {
      errors.push(error.message)
    }
  }

  return { ok: errors.length === 0, request, errors }
}

function validationError (validation) {
  const error = new Error(validation.errors.join('; ') || 'Gateway request is invalid')
  error.code = 'REQUEST_VALIDATION_FAILED'
  error.retryable = false
  error.details = validation.errors
  return error
}

function appendQuery (url, query) {
  if (!isPlainObject(query) || !Object.keys(query).length) return url
  let parsed
  try {
    parsed = new URL(url)
  } catch {
    parsed = null
  }
  const pairs = []
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null) continue
    const values = Array.isArray(value) ? value : [value]
    for (const item of values) {
      if (item !== undefined && item !== null && typeof item !== 'object') pairs.push([key, String(item)])
    }
  }
  if (!pairs.length) return url
  if (parsed) {
    for (const [key, value] of pairs) parsed.searchParams.append(key, value)
    return parsed.toString()
  }
  const separator = String(url).includes('?') ? (String(url).endsWith('?') || String(url).endsWith('&') ? '' : '&') : '?'
  return `${url}${separator}${pairs.map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`).join('&')}`
}

function requestHeaders (headers, payload, context = {}, protectedHeaderNames) {
  if (!isPlainObject(headers)) return {}
  const protectedHeaders = protectedHeaderSet(protectedHeaderNames)
  const result = {}
  for (const [key, value] of Object.entries(headers)) {
    if (protectedHeaders.has(key.toLowerCase())) continue
    if (value === null || value === undefined || typeof value === 'object') continue
    const resolved = typeof value === 'string' && hasUrlTemplate(value)
      ? resolveHeaderTemplate(value, payload, context)
      : value
    if (/[\r\n]/.test(String(resolved))) {
      const error = new Error(`Header ${key} contains an invalid line break`)
      error.code = 'INVALID_HEADER_TEMPLATE'
      error.retryable = false
      throw error
    }
    result[key] = String(resolved)
  }
  return result
}

module.exports = {
  BODY_METHODS,
  HTTP_METHODS,
  appendQuery,
  isPlainObject,
  methodUsesBody,
  normalizeRequest,
  requestEnvelope,
  requestHeaders,
  requestInput,
  requestSource,
  requestSchema,
  validateRequest,
  validationError
}
