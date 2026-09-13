'use strict'

module.exports = function (RED) {
  function GatewayMetricsNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    const serverConfig = RED.nodes.getNode(config.server)
    if (!serverConfig || typeof serverConfig.on !== 'function') {
      node.error('Gateway Server Config is not configured')
      return
    }

    const filters = new Set(
      String(config.events || '')
        .split(',')
        .map(value => value.trim())
        .filter(Boolean)
    )
    const metricListener = metric => {
      if (filters.size && !filters.has(metric.event)) return
      const output = {
        topic: `pod-gateway/${metric.event}`,
        payload: metric,
        metric,
        gateway: {
          event: metric.event,
          requestId: metric.requestId,
          service: metric.service,
          operation: metric.operation,
          operationKey: metric.operationKey,
          status: metric.status || metric.outcome || metric.event,
          timestamp: metric.timestamp
        }
      }
      node.send(output)
    }

    // Gateway Server Config forwards events from the central GatewayServer.
    serverConfig.on('gateway-metric', metricListener)
    node.on('close', (_removed, done) => {
      serverConfig.removeListener?.('gateway-metric', metricListener)
      if (typeof done === 'function') done()
    })
  }

  RED.nodes.registerType('gateway-metrics', GatewayMetricsNode)
}
