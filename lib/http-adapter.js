'use strict'

const { appendQuery, HTTP_METHODS, methodUsesBody, normalizeRequest, requestHeaders } = require('./request-contract')

class UpstreamError extends Error {
  constructor (code, message, options = {}) {
    super(message)
    this.name = 'UpstreamError'
    this.code = code
    this.retryable = Boolean(options.retryable)
    if (options.retryAfterMs != null) this.retryAfterMs = options.retryAfterMs
    if (options.status != null) this.status = options.status
  }
}

function parseRetryAfter (value) {
  if (!value) return undefined
  const seconds = Number(value)
  if (Number.isFinite(seconds)) return Math.max(0, Math.ceil(seconds * 1000))
  const date = Date.parse(value)
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : undefined
}

/**
 * Small adapter factory for APIs that accept JSON. Applications can provide a
 * custom request builder or response mapper while keeping credentials in the
 * gateway process.
 */
function createJsonHttpAdapter (options = {}) {
  if (!options.url && typeof options.buildUrl !== 'function') throw new Error('url or buildUrl is required')
  const method = String(options.method || 'POST').toUpperCase()
  if (!HTTP_METHODS.has(method)) {
    throw new Error(`Unsupported HTTP method ${method}; use GET, POST, PUT, PATCH, DELETE or HEAD`)
  }
  const logger = options.logger || { debug: () => {}, error: () => {} }
  // Any header name(s) this adapter's own configured credentials live under
  // (Gateway API Config's apiKeyHeader) -- a POD must never be able to
  // override those via _request.headers, even when the name is not one of
  // the common ones request-contract.js already knows about.
  const protectedHeaderNames = Array.isArray(options.protectedHeaders) ? options.protectedHeaders : []
  // Node's fetch (undici) already pools and reuses HTTP/1.1 connections to
  // the same origin by default -- keepAlive:true (the default here) is a
  // no-op that just documents that intent. keepAlive:false is the useful
  // case: force `Connection: close` on every request so the upstream tears
  // down the socket instead of it being reused, for providers/load balancers
  // that mishandle persistent connections. Set after the header merge below
  // so a POD's own _request.headers can never turn it back on.
  const keepAlive = options.keepAlive !== false
  return async (payload, context) => {
    const requestContext = context || {}
    const request = normalizeRequest(requestContext.request ?? requestContext._request)
    const baseUrl = typeof options.buildUrl === 'function' ? await options.buildUrl(payload, requestContext) : options.url
    const url = appendQuery(baseUrl, request.query)
    const headers = {
      accept: 'application/json',
      'content-type': 'application/json',
      ...(typeof options.headers === 'function' ? await options.headers(payload, requestContext) : (options.headers || {})),
      ...requestHeaders(request.headers, payload, requestContext, protectedHeaderNames)
    }
    if (options.idempotencyHeader && requestContext.idempotencyKey) {
      headers[options.idempotencyHeader] = requestContext.idempotencyKey
    }
    if (!keepAlive) headers.connection = 'close'
    const defaultBody = Object.prototype.hasOwnProperty.call(request, 'body') && methodUsesBody(method)
      ? request.body
      : payload
    const body = typeof options.buildBody === 'function' ? await options.buildBody(defaultBody, requestContext) : defaultBody
    logger.debug?.(`adapter request method=${method} url=${url} request=${requestContext.requestId || ''}`)
    let response
    try {
      response = await fetch(url, {
        method,
        headers,
        body: method === 'GET' || method === 'HEAD' ? undefined : JSON.stringify(body),
        signal: requestContext.signal
      })
    } catch (error) {
      logger.error?.(`adapter network error request=${requestContext.requestId || ''}: ${error.message}`)
      if (error.name === 'AbortError') throw new UpstreamError('UPSTREAM_TIMEOUT', 'External API request timed out', { retryable: true })
      throw new UpstreamError('UPSTREAM_NETWORK_ERROR', error.message, { retryable: true })
    }

    requestContext.response = { statusCode: Number(response.status) }
    logger.debug?.(`adapter response status=${response.status} request=${requestContext.requestId || ''}`)

    const contentType = response.headers.get('content-type') || ''
    // HEAD responses never carry a body; avoid attempting JSON parsing when
    // an upstream reuses its normal JSON content-type header.
    const result = method === 'HEAD'
      ? null
      : contentType.includes('json') ? await response.json() : await response.text()
    if (!response.ok) {
      const retryable = response.status === 429 || response.status === 502 || response.status === 503 || response.status === 504
      const error = new UpstreamError(`UPSTREAM_HTTP_${response.status}`, `External API returned HTTP ${response.status}`, {
        retryable,
        retryAfterMs: parseRetryAfter(response.headers.get('retry-after')),
        status: response.status
      })
      error.responseBody = result
      throw error
    }
    return typeof options.mapResponse === 'function' ? options.mapResponse(result, response, requestContext) : result
  }
}

module.exports = { UpstreamError, createJsonHttpAdapter, parseRetryAfter }
