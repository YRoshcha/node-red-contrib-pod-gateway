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
  function GatewayCallNode (config) {
    RED.nodes.createNode(this, config)
    const node = this
    node.gatewayId = config.gateway
    node.operation = config.operation
    node.timeout = Number(config.timeout || 60000)
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
        const output = errorMessage(RED, msg, error, { status: 'failed' })
        send([null, output])
        if (done) done()
        return
      }

      const inputPayload = msg.payload
      const requestInput = msg._request
      const requestOptions = normalizeRequest(requestInput)
      const requestId = makeRequestId()
      const timeoutMs = Math.max(1, node.timeout)
      const request = {
        type: 'call',
        requestId,
        operation: operation.value,
        payload: msg.payload,
        _request: requestOptions,
        priority: config.priority || 'normal',
        deadlineAt: new Date(Date.now() + timeoutMs).toISOString(),
        idempotencyKey: msg.gateway?.idempotencyKey || requestId
      }
      node.status({ fill: 'yellow', shape: 'ring', text: 'connecting' })
      const acceptedListener = accepted => {
        if (accepted.requestId !== requestId) return
        node.status({ fill: 'yellow', shape: 'dot', text: accepted.queueId ? `accepted ${accepted.queueId}` : 'accepted' })
      }
      client.on('accepted', acceptedListener)
      try {
        // Do not label a request as queued until the POD WebSocket handshake
        // has completed. This makes a disconnected Gateway Config visible
        // instead of hiding it behind the request deadline.
        await client.connect()
        const capability = client.capabilities?.find(item => item.operation === operation.value)
        const requestValidation = validateRequest(inputPayload, requestInput, capability?.requestSchema, {
          requestId,
          podId: client.podId,
          service: operation.service,
          operation: operation.operation
        })
        if (!requestValidation.ok) throw validationError(requestValidation)
        const method = capability?.requestSchema?.method
        node.status({ fill: 'yellow', shape: 'ring', text: 'queued' })
        const result = await client.request(request, timeoutMs)
        const output = RED.util.cloneMessage(msg)
        output.payload = result.payload
        const statusCode = result.response?.statusCode ?? result.statusCode ?? result._request?.output?.statusCode ?? null
        output.gateway = {
          ...(output.gateway || {}),
          ...gatewayMeta(requestId, operation, 'completed'),
          durationMs: result.durationMs,
          queuedAt: result.queuedAt,
          ...(statusCode ? { httpStatus: Number(statusCode) } : {})
        }
        output._request = requestEnvelope(inputPayload, requestOptions, method, {
          statusCode,
          body: result.payload
        })
        send([output, null])
        node.status({ fill: 'green', shape: 'dot', text: 'completed' })
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
      } finally {
        client.removeListener('accepted', acceptedListener)
      }
    })
  }

  RED.nodes.registerType('gateway-call', GatewayCallNode)
}
