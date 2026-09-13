'use strict'

const { createJsonHttpAdapter } = require('../lib/http-adapter')
const { parseHeaders } = require('../lib/api-config')
const { hasUrlTemplate, resolveUrlTemplate } = require('../lib/url-template')
const { requestSchema } = require('../lib/request-contract')

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

    const adapterOptions = {
      method: config.method || 'POST',
      headers,
      idempotencyHeader: config.idempotencyHeader || undefined,
      keepAlive: config.keepAlive !== false,
      logger: server.logger,
      ...(api?.apiKeyHeader ? { protectedHeaders: [api.apiKeyHeader] } : {})
    }
    if (buildUrl) adapterOptions.buildUrl = buildUrl
    else adapterOptions.url = url

    node.ready = server.registerOperation(config.operation, createJsonHttpAdapter(adapterOptions), metadata).then(() => {
      node.status({ fill: 'green', shape: 'dot', text: 'registered' })
    }).catch(error => {
      node.status({ fill: 'red', shape: 'ring', text: 'registration failed' })
      node.error(`Gateway adapter registration failed: ${error.message}`)
      // The server config reports the startup error. Do not leave an
      // unhandled rejection that could terminate the Node-RED process.
      return false
    })

    node.on('close', async (_removed, done) => {
      try {
        await node.ready.catch(() => {})
        await server.unregisterOperation(config.operation).catch(() => {})
      } finally {
        if (typeof done === 'function') done()
      }
    })
  }

  RED.nodes.registerType('pod-gateway-adapter', GatewayAdapterNode)
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
