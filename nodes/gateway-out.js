'use strict'

const { makeRequestId } = require('../lib/protocol')
const {
  getClient,
  setConnectionStatus,
  operationFromNode,
  gatewayMeta,
  errorMessage,
  closeListener
} = require('../lib/node-utils')
const {
  normalizeRequest,
  requestEnvelope,
  validateRequest,
  validationError
} = require('../lib/request-contract')

module.exports = function (RED) {
  function GatewayOutNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    node.gatewayId = config.gateway
    node.operation = config.operation
    node.acceptTimeout = Number(config.acceptTimeout || 10000)
    const gateway = getClient(RED, node.gatewayId, node)
    if (!gateway) return
    const client = gateway.client
    const stateListener = state => setConnectionStatus(node, state)
    client.on('state', stateListener)
    closeListener(node, client, 'state', stateListener)
    setConnectionStatus(node, client.state)

    node.on('input', async (msg, send, done) => {
      let operation
      try {
        operation = operationFromNode(node, msg)
      } catch (error) {
        send([null, errorMessage(RED, msg, error, { status: 'failed' })])
        if (done) done()
        return
      }
      const inputPayload = msg.payload
      const requestInput = msg._request
      const requestOptions = normalizeRequest(requestInput)
      const requestId = makeRequestId()
      const request = {
        type: 'event',
        requestId,
        operation: operation.value,
        payload: msg.payload,
        _request: requestOptions,
        priority: config.priority || 'bulk',
        idempotencyKey: msg.gateway?.idempotencyKey || requestId
      }
      node.status({ fill: 'yellow', shape: 'ring', text: 'sending' })
      try {
        await client.connect()
        const capability = client.capabilities?.find(item => item.operation === operation.value)
        const requestValidation = validateRequest(inputPayload, requestInput, capability?.requestSchema, {
          requestId,
          podId: client.podId,
          service: operation.service,
          operation: operation.operation
        })
        if (!requestValidation.ok) throw validationError(requestValidation)
        await client.send(request, node.acceptTimeout)
        const accepted = RED.util.cloneMessage(msg)
        accepted._request = requestEnvelope(inputPayload, requestOptions, capability?.requestSchema?.method, {
          status: 'accepted'
        })
        accepted.gateway = {
          ...(accepted.gateway || {}),
          ...gatewayMeta(requestId, operation, 'accepted')
        }
        send([accepted, null])
        node.status({ fill: 'green', shape: 'dot', text: 'accepted' })
        if (done) done()
      } catch (error) {
        const output = errorMessage(RED, msg, error, gatewayMeta(requestId, operation, 'failed'))
        const capability = client.capabilities?.find(item => item.operation === operation.value)
        const statusCode = error.statusCode ?? error.status ?? null
        output._request = requestEnvelope(inputPayload, requestOptions, capability?.requestSchema?.method, {
          statusCode,
          ...(error.responseBody !== undefined ? { body: error.responseBody } : {}),
          error: output.error
        })
        if (statusCode) output.gateway.httpStatus = Number(statusCode)
        send([null, output])
        node.status({ fill: 'red', shape: 'ring', text: error.code || 'failed' })
        if (done) done()
      }
    })
  }

  RED.nodes.registerType('gateway-out', GatewayOutNode)
}
