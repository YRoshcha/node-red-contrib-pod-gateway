'use strict'

const { GatewayClient } = require('../lib/gateway-client')
const { createLogger } = require('../lib/logger')

module.exports = function (RED) {
  if (RED.httpAdmin?.get) {
    const permission = RED.auth?.needsPermission ? RED.auth.needsPermission('nodes.read') : (_req, _res, next) => next()
    RED.httpAdmin.get('/pod-gateway/capabilities/:id', permission, async (request, response) => {
      const config = RED.nodes.getNode(request.params.id)
      if (!config?.client) return response.status(404).json({ error: 'Gateway Config not found' })
      try {
        await config.client.connect()
        response.json(config.capabilities || [])
      } catch (error) {
        response.status(503).json({ error: error.code || 'GATEWAY_UNAVAILABLE', message: error.message })
      }
    })
  }

  function GatewayConfigNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    const logger = createLogger(RED.log, { verbose: config.verboseLogging })
    const token = node.credentials?.token || process.env.POD_GATEWAY_TOKEN
    node.client = new GatewayClient({
      url: config.url,
      token,
      podId: config.podId,
      reconnectInterval: config.reconnectInterval,
      connectTimeout: config.connectTimeout,
      heartbeatInterval: config.heartbeatInterval,
      logger
    })
    node.capabilities = []

    node.client.on('state', state => node.emit('gateway-state', state))
    node.client.on('capabilities', capabilities => {
      node.capabilities = capabilities
      node.emit('gateway-capabilities', capabilities)
    })
    node.client.on('error', error => node.emit('gateway-error', error))

    // Connect lazily when the first functional node sends a message. This keeps
    // a Node-RED deployment from opening unused gateway connections.
    node.on('close', (_removed, done) => {
      node.client.close()
      if (typeof done === 'function') done()
    })
  }

  RED.nodes.registerType('gateway-config', GatewayConfigNode, {
    credentials: {
      token: { type: 'password' }
    }
  })
}
