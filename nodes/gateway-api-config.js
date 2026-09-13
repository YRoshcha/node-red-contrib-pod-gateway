'use strict'

const { parseHeaders, requestOptions } = require('../lib/api-config')

module.exports = function (RED) {
  function GatewayApiConfigNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    node.baseUrl = config.baseUrl || ''
    node.apiKeyHeader = config.apiKeyHeader || ''
    node.apiKeyPrefix = config.apiKeyPrefix || ''
    node.apiKey = node.credentials?.apiKey || ''
    node.headers = {}
    node.configError = null

    try {
      node.headers = parseHeaders(config.headers)
      // Validate early so a bad base URL is shown on the config node.
      requestOptions({
        baseUrl: node.baseUrl,
        headers: node.headers,
        apiKeyHeader: node.apiKeyHeader,
        apiKeyPrefix: node.apiKeyPrefix,
        apiKey: node.apiKey
      }, '')
      node.status({ fill: 'green', shape: 'dot', text: 'configured' })
    } catch (error) {
      node.configError = error
      node.status({ fill: 'red', shape: 'ring', text: 'invalid API config' })
      node.error(error.message)
    }

    node.buildRequestOptions = (path, extraHeaders) => {
      if (node.configError) throw node.configError
      return requestOptions({
        baseUrl: node.baseUrl,
        headers: node.headers,
        apiKeyHeader: node.apiKeyHeader,
        apiKeyPrefix: node.apiKeyPrefix,
        apiKey: node.apiKey
      }, path, extraHeaders)
    }
  }

  RED.nodes.registerType('pod-gateway-api-config', GatewayApiConfigNode, {
    credentials: {
      apiKey: { type: 'password' }
    }
  })
}
