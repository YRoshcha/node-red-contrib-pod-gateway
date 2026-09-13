'use strict'

const {
  getClient,
  setConnectionStatus,
  gatewayMeta
} = require('../lib/node-utils')

module.exports = function (RED) {
  function GatewayInNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    const gateway = getClient(RED, config.gateway, node)
    if (!gateway) return
    const client = gateway.client
    const eventFilter = (config.event || '').trim()
    const stateListener = state => setConnectionStatus(node, state)
    const messageListener = message => {
      const operation = message.operation || ''
      if (eventFilter && eventFilter !== operation && eventFilter !== message.event) return
      const output = {
        payload: message.payload,
        ...(message._request ? { _request: message._request } : {}),
        gateway: gatewayMeta(
          message.requestId || message.messageId,
          operation.includes('/') ? {
            service: operation.split('/')[0],
            operation: operation.split('/').slice(1).join('/'),
            value: operation
          } : { service: '', operation, value: operation },
          message.ok === false ? 'failed' : 'received',
          { event: message.event, receivedAt: message.receivedAt }
        )
      }
      const statusCode = message.response?.statusCode ?? message.statusCode ?? message._request?.output?.statusCode
      if (statusCode != null) output.gateway.httpStatus = Number(statusCode)
      if (message.error) output.error = message.error
      node.send(output)
    }
    client.on('state', stateListener)
    client.on('message', messageListener)
    node.on('close', (_removed, done) => {
      client.removeListener('state', stateListener)
      client.removeListener('message', messageListener)
      if (typeof done === 'function') done()
    })
    setConnectionStatus(node, client.state)
    client.connect().catch(error => {
      node.status({ fill: 'red', shape: 'ring', text: error.code || 'disconnected' })
    })
  }

  RED.nodes.registerType('gateway-in', GatewayInNode)
}
