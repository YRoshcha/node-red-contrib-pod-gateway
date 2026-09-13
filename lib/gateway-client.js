'use strict'

const { EventEmitter } = require('node:events')
const { randomUUID } = require('node:crypto')
const WebSocket = require('ws')
const { PROTOCOL_VERSION, makeRequestId, errorPayload } = require('./protocol')

class GatewayClient extends EventEmitter {
  constructor (options = {}) {
    super()
    this.url = options.url
    this.token = options.token
    this.podId = options.podId || process.env.POD_ID || randomUUID()
    this.instanceId = options.instanceId || randomUUID()
    this.connectTimeout = Number(options.connectTimeout || 10000)
    this.reconnectInterval = Number(options.reconnectInterval || 3000)
    this.heartbeatInterval = Number(options.heartbeatInterval || 25000)
    this.logger = options.logger || console
    this.ws = null
    this.state = 'disconnected'
    this.pending = new Map()
    this.aliases = new Map()
    this.capabilities = []
    this.connectPromise = null
    this.reconnectTimer = null
    this.heartbeatTimer = null
    this.closing = false
    this.authFailed = false
  }

  connect () {
    if (this.closing) return Promise.reject(this._error('GATEWAY_CLOSED', 'Gateway client is closed'))
    if (this.state === 'connected' && this.ws?.readyState === WebSocket.OPEN) {
      this.logger.debug?.(`client already connected pod=${this.podId}`)
      return Promise.resolve()
    }
    if (this.connectPromise) return this.connectPromise
    if (!this.url) return Promise.reject(this._error('GATEWAY_URL_MISSING', 'Gateway URL is not configured'))

    this.logger.debug?.(`client connecting url=${this.url} pod=${this.podId}`)
    this._setState('connecting')
    this.connectPromise = new Promise((resolve, reject) => {
      let settled = false
      const onHelloAck = message => {
        if (settled) return
        clearTimeout(timer)
        settled = true
        this.removeListener('hello_error', onHelloError)
        this.connectPromise = null
        this.authFailed = false
        this._setState('connected')
        this._startHeartbeat()
        this.capabilities = message.capabilities || []
        this.emit('capabilities', this.capabilities)
        this.logger.debug?.(`client hello acknowledged pod=${this.podId} capabilities=${this.capabilities.length}`)
        resolve()
      }
      const onHelloError = message => {
        if (settled) return
        clearTimeout(timer)
        settled = true
        this.removeListener('hello_ack', onHelloAck)
        this.connectPromise = null
        this.authFailed = true
        try { this.ws?.close() } catch {}
        const error = this._error(message.error?.code || 'GATEWAY_AUTH_FAILED', message.error?.message || 'Gateway handshake failed')
        this.logger.error?.(`client hello failed pod=${this.podId} code=${error.code}`)
        this._setState('disconnected')
        this.logger.error?.(`client connect timeout url=${this.url} pod=${this.podId}`)
        reject(error)
      }
      const timer = setTimeout(() => {
        if (settled) return
        settled = true
        this.removeListener('hello_ack', onHelloAck)
        this.removeListener('hello_error', onHelloError)
        try { this.ws?.terminate() } catch {}
        this.connectPromise = null
        this._setState('disconnected')
        reject(this._error('GATEWAY_CONNECT_TIMEOUT', 'Gateway connection timed out', true))
        if (!this.authFailed) this._scheduleReconnect()
      }, this.connectTimeout)

      let ws
      try {
        ws = new WebSocket(this.url)
      } catch (error) {
        clearTimeout(timer)
        settled = true
        this.connectPromise = null
        this._setState('disconnected')
        reject(error)
        this._scheduleReconnect()
        return
      }
      this.ws = ws

      ws.on('open', () => {
        this.logger.debug?.(`client websocket open pod=${this.podId}`)
        this._sendRaw({
          type: 'hello',
          protocolVersion: PROTOCOL_VERSION,
          podId: this.podId,
          instanceId: this.instanceId,
          token: this.token
        })
      })

      ws.on('message', data => this._handleMessage(data))
      ws.on('error', error => {
        this.logger.error?.(`client websocket error pod=${this.podId}: ${error.message}`)
        this.emit('error', error)
      })
      ws.on('close', () => {
        clearTimeout(timer)
        this.removeListener('hello_ack', onHelloAck)
        this.removeListener('hello_error', onHelloError)
        this._stopHeartbeat()
        const wasConnected = this.state === 'connected'
        this.ws = null
        this.connectPromise = null
        this._setState('disconnected')
        this.logger.debug?.(`client websocket closed pod=${this.podId} wasConnected=${wasConnected}`)
        if (!settled) {
          settled = true
          reject(this._error('GATEWAY_CONNECTION_LOST', 'Gateway connection closed during handshake', true))
        }
        if (wasConnected) this._rejectPending(this._error('GATEWAY_CONNECTION_LOST', 'Gateway connection lost', true))
        if (!this.authFailed) this._scheduleReconnect()
      })

      this.once('hello_ack', onHelloAck)
      this.once('hello_error', onHelloError)
    })
    return this.connectPromise
  }

  async send (message, timeoutMs = 10000) {
    await this.connect()
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) throw this._error('GATEWAY_NOT_CONNECTED', 'Gateway is not connected', true)
    const requestId = message.requestId || makeRequestId()
    const envelope = { ...message, requestId }
    this.logger.debug?.(`client send type=${envelope.type} operation=${envelope.operation || ''} request=${requestId}`)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId)
        reject(this._error('GATEWAY_ACCEPT_TIMEOUT', 'Gateway did not accept the message in time', true))
      }, timeoutMs)
      this.pending.set(requestId, { resolveAccepted: resolve, reject, timer, request: envelope, waitForResult: false })
      this._sendRaw(envelope)
    })
  }

  async request (message, timeoutMs = 60000, options = {}) {
    await this.connect()
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) throw this._error('GATEWAY_NOT_CONNECTED', 'Gateway is not connected', true)
    const requestId = message.requestId || makeRequestId()
    const envelope = { ...message, requestId }
    this.logger.debug?.(`client request operation=${envelope.operation || ''} request=${requestId} timeoutMs=${timeoutMs}`)
    return new Promise((resolve, reject) => {
      const acceptTimeoutMs = Math.max(1, Math.min(
        Number(options.acceptTimeoutMs || 10000),
        Number(timeoutMs)
      ))
      const timer = setTimeout(() => {
        const pending = this.pending.get(requestId)
        this.pending.delete(requestId)
        this.logger.error?.(`client result timeout request=${requestId} accepted=${Boolean(pending?.accepted)}`)
        if (pending?.accepted) {
          reject(this._error('GATEWAY_RESULT_TIMEOUT', 'Gateway accepted the call but did not return a result', true))
        } else {
          reject(this._error('DEADLINE_EXCEEDED', 'Gateway call deadline exceeded', true))
        }
      }, timeoutMs)
      const acceptedTimer = setTimeout(() => {
        const pending = this.pending.get(requestId)
        if (!pending || pending.accepted) return
        clearTimeout(pending.timer)
        this.pending.delete(requestId)
        this.logger.error?.(`client accept timeout request=${requestId}`)
        reject(this._error('GATEWAY_ACCEPT_TIMEOUT', 'Gateway did not accept the call in time', true))
      }, acceptTimeoutMs)
      this.pending.set(requestId, { resolve, reject, timer, acceptedTimer, request: envelope, waitForResult: true })
      this._sendRaw(envelope)
    })
  }

  close () {
    this.logger.debug?.(`client closing pod=${this.podId}`)
    this.closing = true
    clearTimeout(this.reconnectTimer)
    this._stopHeartbeat()
    this._rejectPending(this._error('GATEWAY_CLOSED', 'Gateway client closed'))
    try { this.ws?.close() } catch {}
    this.ws = null
    this._setState('closed')
  }

  _handleMessage (data) {
    let message
    try { message = JSON.parse(data.toString()) } catch {
      this.emit('error', this._error('INVALID_GATEWAY_MESSAGE', 'Gateway sent invalid JSON'))
      return
    }
    if (message.type === 'hello_ack') return this.emit('hello_ack', message)
    if (message.type === 'hello_error') return this.emit('hello_error', message)
    if (message.type === 'capabilities') {
      this.capabilities = message.items || []
      return this.emit('capabilities', this.capabilities)
    }
    if (message.type === 'heartbeat') return this.emit('heartbeat', message)

    this.logger.debug?.(`client received type=${message.type || 'unknown'} operation=${message.operation || ''} request=${message.requestId || ''}`)

    const requestId = message.requestId
    const pending = requestId ? this.pending.get(requestId) : null
    if (message.type === 'accepted' && pending) {
      if (pending.waitForResult) {
        clearTimeout(pending.acceptedTimer)
        pending.accepted = message
        this.logger.debug?.(`client accepted request=${requestId} queueId=${message.queueId || ''}`)
        if (message.duplicateOf) {
          this.pending.delete(requestId)
          if (!this.aliases.has(message.duplicateOf)) this.aliases.set(message.duplicateOf, [])
          this.aliases.get(message.duplicateOf).push({ ...pending, requestId })
        }
      } else {
        clearTimeout(pending.timer)
        this.pending.delete(requestId)
        pending.resolveAccepted(message)
      }
      this.emit('accepted', message)
      return
    }
    if ((message.type === 'result' || message.type === 'failed') && pending) {
      clearTimeout(pending.timer)
      clearTimeout(pending.acceptedTimer)
      this.pending.delete(requestId)
      const aliases = this.aliases.get(requestId) || []
      this.aliases.delete(requestId)
      const error = message.type === 'result' && message.ok !== false
        ? null
        : this._error(message.error?.code || 'GATEWAY_REQUEST_FAILED', message.error?.message || 'Gateway request failed', Boolean(message.error?.retryable), message.error?.retryAfterMs, message.response?.statusCode ?? message._request?.output?.statusCode)
      if (error && Array.isArray(message.error?.details)) error.details = message.error.details
      if (error && message._request?.output?.body !== undefined) error.responseBody = message._request.output.body
      if (error) pending.reject(error)
      else pending.resolve(message)
      this.logger.debug?.(`client result request=${requestId} ok=${!error} code=${error?.code || ''}`)
      for (const alias of aliases) {
        clearTimeout(alias.timer)
        if (error) alias.reject(error)
        else alias.resolve({ ...message, requestId: alias.requestId, duplicateOf: requestId })
      }
      this.emit('result', message)
      return
    }
    if (message.type === 'event' || message.type === 'result' || message.type === 'failed') this.emit('message', message)
  }

  _sendRaw (message) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) throw this._error('GATEWAY_NOT_CONNECTED', 'Gateway is not connected', true)
    this.ws.send(JSON.stringify(message))
  }

  _startHeartbeat () {
    this._stopHeartbeat()
    this.heartbeatTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        try { this._sendRaw({ type: 'heartbeat', timestamp: new Date().toISOString() }) } catch {}
      }
    }, this.heartbeatInterval)
  }

  _stopHeartbeat () {
    clearInterval(this.heartbeatTimer)
    this.heartbeatTimer = null
  }

  _scheduleReconnect () {
    if (this.closing || this.authFailed || this.reconnectTimer) return
    // A gateway restart or a shared network blip disconnects every POD at
    // once. Without jitter they would all retry after exactly the same
    // reconnectInterval and hit the gateway's WebSocket handshake in one
    // synchronized burst; +/-30% jitter spreads that burst out.
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.connect().catch(() => {})
    }, this._reconnectDelay())
  }

  _reconnectDelay () {
    const jitterRatio = 0.3
    const jitter = this.reconnectInterval * jitterRatio * (Math.random() * 2 - 1)
    return Math.max(0, Math.round(this.reconnectInterval + jitter))
  }

  _rejectPending (error) {
    for (const [requestId, pending] of this.pending) {
      clearTimeout(pending.timer)
      clearTimeout(pending.acceptedTimer)
      pending.reject(error)
      this.pending.delete(requestId)
    }
    for (const aliases of this.aliases.values()) {
      for (const pending of aliases) {
        clearTimeout(pending.timer)
        pending.reject(error)
      }
    }
    this.aliases.clear()
  }

  _setState (state) {
    if (this.state === state) return
    this.state = state
    this.logger.debug?.(`client state=${state} pod=${this.podId}`)
    this.emit('state', state)
  }

  _error (code, message, retryable = false, retryAfterMs, statusCode) {
    const error = new Error(message)
    error.code = code
    error.retryable = retryable
    if (retryAfterMs != null) error.retryAfterMs = retryAfterMs
    if (statusCode != null) error.statusCode = Number(statusCode)
    return error
  }
}

module.exports = { GatewayClient }
