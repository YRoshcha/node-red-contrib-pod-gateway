'use strict'

const { createJsonHttpAdapter } = require('../lib/http-adapter')
const { parseHeaders } = require('../lib/api-config')
const { hasUrlTemplate, resolveUrlTemplate, resolveHeaderTemplate } = require('../lib/url-template')
const { requestSchema } = require('../lib/request-contract')
const { globalContextAccessor } = require('../lib/node-utils')

module.exports = function (RED) {
  function GatewayAdapterNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    const server = RED.nodes.getNode(config.server)
    const api = config.api ? RED.nodes.getNode(config.api) : null
    node.serverId = config.server
    node.operation = config.operation

    if (!server) {
      node.status({ fill: 'red', shape: 'ring', text: 'server config missing' })
      node.error('Gateway Server Config is required')
      return
    }

    let headers
    let url
    let buildUrl
    let rateLimit
    let schema
    try {
      headers = parseHeaders(config.headers)
      rateLimit = parseRateLimit(config)
      if (api) {
        const path = String(config.path || '')
        // API Config owns provider-wide headers and credentials. Resolve a
        // templated path for each request while keeping those headers static.
        const options = api.buildRequestOptions('', headers)
        headers = options.headers
        if (hasUrlTemplate(path)) {
          buildUrl = (payload, context) => api.buildRequestOptions(resolveUrlTemplate(path, payload, context), {}).url
        } else {
          url = api.buildRequestOptions(path, headers).url
        }
      } else if (!config.url) {
        throw new Error('Gateway API Config or direct upstream URL is required')
      } else if (hasUrlTemplate(config.url)) {
        buildUrl = (payload, context) => resolveUrlTemplate(config.url, payload, context)
      } else {
        url = config.url
      }
      schema = requestSchema(config.method || 'POST', api ? config.path : config.url)
      // The API Config's own credential header (if any) must never be
      // overridable by a POD's _request.headers, even when it is a custom
      // header name request-contract.js's static protected list doesn't
      // already know about.
      if (api?.apiKeyHeader) schema.protectedHeaders = [api.apiKeyHeader]
    } catch (error) {
      node.status({ fill: 'red', shape: 'ring', text: 'invalid adapter config' })
      node.error(error.message)
      return
    }

    const metadata = {
      label: config.label || config.operation,
      description: config.description || '',
      requestSchema: schema
    }
    if (rateLimit) metadata.rateLimit = rateLimit

    // API Config and Adapter headers are normally merged once, above, and
    // stay static for the life of the deploy. A header value containing
    // {{...}} (most usefully {{global.apiToken}}, but {{payload.x}} and
    // {{request.x}} work the same as in _request.headers) instead needs to
    // be resolved on every call, so it is not baked in at deploy time.
    const headerTemplates = headers
    const hasHeaderTemplates = Object.values(headerTemplates).some(
      value => typeof value === 'string' && hasUrlTemplate(value)
    )
    const globalContext = hasHeaderTemplates ? globalContextAccessor(node) : null
    const resolvedHeaders = hasHeaderTemplates
      ? (payload, requestContext) => resolveConfigHeaders(headerTemplates, payload, requestContext, globalContext)
      : headers

    const adapterOptions = {
      method: config.method || 'POST',
      headers: resolvedHeaders,
      idempotencyHeader: config.idempotencyHeader || undefined,
      keepAlive: config.keepAlive !== false,
      logger: server.logger,
      ...(api?.apiKeyHeader ? { protectedHeaders: [api.apiKeyHeader] } : {})
    }
    if (buildUrl) adapterOptions.buildUrl = buildUrl
    else adapterOptions.url = url

    try {
      server.registerOperation(config.operation, createJsonHttpAdapter(adapterOptions), metadata)
      node.status({ fill: 'green', shape: 'dot', text: 'registered' })
      node.ready = Promise.resolve()
    } catch (error) {
      node.status({ fill: 'red', shape: 'ring', text: 'registration failed' })
      node.error(`Gateway adapter registration failed: ${error.message}`)
      node.ready = Promise.resolve(false)
    }

    node.on('close', (_removed, done) => {
      try {
        server.unregisterOperation(config.operation)
      } catch (error) {
        // Already gone (e.g. server itself shutting down) -- nothing to do.
      }
      if (typeof done === 'function') done()
    })
  }

  RED.nodes.registerType('pod-gateway-adapter', GatewayAdapterNode)
}

/**
 * Resolve {{...}} templates in API Config / Adapter headers for one
 * request. Unlike _request.headers from a POD, these are operator-authored
 * config, not untrusted per-call input, so there is no protected-header
 * list to enforce here -- an operator can legitimately put a credential
 * behind {{global.apiToken}}. A missing or non-scalar variable fails the
 * call with INVALID_HEADER_TEMPLATE (non-retryable) via the same error path
 * as a bad _request.headers template, surfaced to the POD as a normal
 * gateway error rather than crashing the adapter.
 */
function resolveConfigHeaders (headerTemplates, payload, requestContext, globalContext) {
  const resolved = {}
  const context = { ...requestContext, global: globalContext || {} }
  for (const [key, value] of Object.entries(headerTemplates)) {
    resolved[key] = typeof value === 'string' && hasUrlTemplate(value)
      ? resolveHeaderTemplate(value, payload, context)
      : value
  }
  return resolved
}

function parseRateLimit (config) {
  const rawRate = String(config.rate ?? '').trim()
  if (!rawRate) return null
  const rate = Number(rawRate)
  if (!Number.isFinite(rate) || rate <= 0) throw new Error('Rate limit must be a positive number')
  const rawBurst = String(config.burst ?? '').trim()
  const burst = rawBurst ? Number(rawBurst) : 1
  if (!Number.isInteger(burst) || burst < 1) throw new Error('Burst must be a positive integer')
  if (burst < rate) throw new Error('Burst must be greater than or equal to rate')
  const ratePerSecond = config.rateUnit === 'minute' ? rate / 60 : rate
  if (!Number.isFinite(ratePerSecond) || ratePerSecond <= 0) throw new Error('Rate limit is too small')
  return { rate: ratePerSecond, burst }
}
