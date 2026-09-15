'use strict'

const http = require('node:http')
const { randomUUID } = require('node:crypto')
const { EventEmitter } = require('node:events')
const { WebSocketServer, WebSocket } = require('ws')
const {
  PROTOCOL_VERSION,
  PRIORITY,
  validateEnvelope,
  parseOperation,
  errorPayload
} = require('./protocol')
const {
  normalizeRequest,
  requestEnvelope,
  validateRequest,
  validationError
} = require('./request-contract')

class GatewayServer extends EventEmitter {
  constructor (options = {}) {
    super()
    this.host = options.host || '0.0.0.0'
    this.port = Number(options.port ?? process.env.PORT ?? 8080)
    this.path = options.path || '/ws'
    this.httpServer = options.server || http.createServer((request, response) => this._health(request, response))
    this.ownsServer = !options.server
    this.wss = new WebSocketServer({ server: this.httpServer, path: this.path })
    this.queue = options.queue || null
    this.rateLimiter = options.rateLimiter
    this.adapters = new Map(Object.entries(options.adapters || {}))
    this.authenticate = options.authenticate || (async () => true)
    this.maxQueueSize = Number(options.maxQueueSize || 10000)
    this.maxMessageBytes = Number(options.maxMessageBytes || 1024 * 1024)
    this.concurrency = Number(options.concurrency || 32)
    this.upstreamTimeoutMs = Number(options.upstreamTimeoutMs || 60000)
    this.maxAttempts = Number(options.maxAttempts || 0)
    this.resultTtlSeconds = positiveInteger(options.resultTtlSeconds, 3600)
    this.logger = options.logger || console
    this.memoryQueue = []
    this.active = 0
    this.pumping = false
    this.pumpTimer = null
    this.connections = new Map()
    this.connectionsByPod = new Map()
    this.pendingResults = new Map()
    this.inflightIdempotency = new Map()
    this.idempotencyResults = new Map()
    // Warn at most once per operation, not once per request, if its upstream
    // timeout can exceed the queue's claimIdleMs -- see _checkClaimIdleMargin.
    this._claimIdleWarned = new Set()
    this.started = false

    this.logger.debug?.(`server constructed port=${this.port} path=${options.path || '/ws'} queue=${Boolean(this.queue)}`)

    this.wss.on('connection', (ws, request) => this._connection(ws, request))
    this.wss.on('error', error => this.logger.error?.('[gateway] websocket server error', error))
  }

  registerOperation (operation, handler, metadata = {}) {
    parseOperation(operation)
    this.adapters.set(operation, { execute: handler, ...metadata })
    this._scheduleCapabilitiesBroadcast()
  }

  unregisterOperation (operation) {
    parseOperation(operation)
    const removed = this.adapters.delete(operation)
    if (removed) this._scheduleCapabilitiesBroadcast()
    return removed
  }

  _scheduleCapabilitiesBroadcast () {
    if (this._capabilitiesBroadcastPending) return
    this._capabilitiesBroadcastPending = true
    process.nextTick(() => {
      this._capabilitiesBroadcastPending = false
      this._broadcastCapabilities()
    })
  }

  _broadcastCapabilities () {
    const items = this.capabilities()
    let sent = 0
    for (const connection of this.connections.values()) {
      if (!connection.authenticated) continue
      this._send(connection, { type: 'capabilities', items })
      sent++
    }
    if (sent) this.logger.debug?.(`capabilities broadcast connections=${sent} operations=${items.length}`)
  }

  capabilities () {
    return [...this.adapters.entries()].map(([operation, value]) => ({
      operation,
      ...(value?.label ? { label: value.label } : {}),
      ...(value?.description ? { description: value.description } : {}),
      ...(value?.inputSchema ? { inputSchema: value.inputSchema } : {}),
      ...(value?.requestSchema ? { requestSchema: value.requestSchema } : {})
    }))
  }

  async start () {
    if (this.started) return Promise.resolve()
    this.logger.debug?.(`server starting port=${this.port}`)
    this.started = true
    if (this.ownsServer) {
      await new Promise((resolve, reject) => {
        const onError = error => {
          this.httpServer.removeListener('listening', onListening)
          reject(error)
        }
        const onListening = () => {
          this.httpServer.removeListener('error', onError)
          resolve()
        }
        this.httpServer.once('error', onError)
        this.httpServer.once('listening', onListening)
        this.httpServer.listen(this.port, this.host)
      })
    }
    if (this.queue) {
      await this.queue.start((item, queueMeta) => this._handleQueuedItem(item, queueMeta))
    }
    this.logger.info?.(`server started port=${this.port} path=${this.path}`)
  }

  async stop () {
    this.logger.debug?.('server stopping')
    clearTimeout(this.pumpTimer)
    for (const connection of this.connections.values()) {
      try { connection.ws.close(1001, 'gateway stopping') } catch {}
    }
    await new Promise(resolve => this.wss.close(() => resolve()))
    if (this.ownsServer && this.httpServer.listening) {
      await new Promise(resolve => this.httpServer.close(() => resolve()))
    }
    if (typeof this.queue?.stop === 'function') await this.queue.stop()
    if (typeof this.rateLimiter?.close === 'function') await this.rateLimiter.close()
    this.started = false
  }

  _connection (ws, request) {
    const connection = {
      ws,
      request,
      authenticated: false,
      connectionId: randomUUID(),
      podId: null,
      instanceId: null
    }
    this.connections.set(ws, connection)
    this.logger.debug?.(`ws connection opened connection=${connection.connectionId} remote=${request?.socket?.remoteAddress || 'unknown'}`)
    ws.on('message', data => this._message(connection, data))
    ws.on('error', error => this.logger.warn?.('[gateway] client websocket error', error.message))
    ws.on('close', () => this._disconnect(connection))
  }

  async _message (connection, data) {
    if (Buffer.byteLength(data) > this.maxMessageBytes) {
      return this._send(connection, this._failed('', 'MESSAGE_TOO_LARGE', 'Gateway message exceeds the configured size limit'))
    }
    let message
    try {
      message = JSON.parse(data.toString())
    } catch {
      return this._send(connection, this._failed('', 'INVALID_MESSAGE', 'Message must be valid JSON'))
    }

    this.logger.debug?.(`ws message type=${message.type || 'unknown'} request=${message.requestId || ''} connection=${connection.connectionId}`)

    if (message.type === 'hello') return this._hello(connection, message)
    if (message.type === 'heartbeat') {
      return this._send(connection, { type: 'heartbeat', timestamp: new Date().toISOString() })
    }
    if (!connection.authenticated) {
      return this._send(connection, this._failed(message.requestId, 'NOT_AUTHENTICATED', 'Send hello before requests'))
    }
    if (!['call', 'event'].includes(message.type)) {
      return this._send(connection, this._failed(message.requestId, 'UNSUPPORTED_MESSAGE', `Unsupported message type: ${message.type}`))
    }

    try { validateEnvelope(message) } catch (error) {
      return this._send(connection, this._failed(message.requestId, 'INVALID_REQUEST', error.message))
    }
    return this._enqueue(connection, message)
  }

  async _hello (connection, message) {
    if (connection.authenticated) return
    this.logger.debug?.(`ws hello pod=${message.podId || ''} instance=${message.instanceId || ''}`)
    if (message.protocolVersion && message.protocolVersion !== PROTOCOL_VERSION) {
      return this._send(connection, {
        type: 'hello_error',
        error: { code: 'UNSUPPORTED_PROTOCOL', message: `Supported protocol is ${PROTOCOL_VERSION}` }
      })
    }
    try {
      const accepted = await this.authenticate(message, connection.request)
      if (!accepted) throw Object.assign(new Error('Gateway authentication failed'), { code: 'AUTH_FAILED' })
    } catch (error) {
      this._send(connection, { type: 'hello_error', error: errorPayload(error, { code: 'AUTH_FAILED' }) })
      try { connection.ws.close(1008, 'authentication failed') } catch {}
      return
    }
    if (!message.podId) {
      return this._send(connection, { type: 'hello_error', error: { code: 'POD_ID_MISSING', message: 'podId is required' } })
    }
    connection.authenticated = true
    connection.podId = message.podId
    connection.instanceId = message.instanceId || message.podId
    this.connectionsByPod.set(connection.podId, connection)
    this._send(connection, {
      type: 'hello_ack',
      protocolVersion: PROTOCOL_VERSION,
      connectionId: connection.connectionId,
      capabilities: this.capabilities()
    })
    this.logger.debug?.(`ws hello acknowledged pod=${connection.podId} connection=${connection.connectionId} capabilities=${this.capabilities().length}`)
    this._replayPending(connection).catch(error => this.logger.warn?.('[gateway] pending result replay failed', error.message))
  }

  async _enqueue (connection, message) {
    const operation = parseOperation(message.operation)
    if (!this.adapters.has(operation.value)) {
      this._emitMetric('request.rejected', null, {
        requestId: message.requestId,
        podId: connection.podId,
        service: operation.service,
        operation: operation.operation,
        operationKey: operation.value,
        outcome: 'error',
        status: 'failed',
        errorCode: 'OPERATION_NOT_FOUND'
      })
      return this._send(connection, this._failed(message.requestId, 'OPERATION_NOT_FOUND', `Operation is not registered: ${operation.value}`))
    }
    const registered = this.adapters.get(operation.value)
    const registeredOperation = {
      ...operation,
      ...(registered?.rateLimit ? { rateLimit: registered.rateLimit } : {}),
      ...(registered?.requestSchema ? { requestSchema: registered.requestSchema } : {})
    }
    const requestValidation = validateRequest(message.payload, message._request, registered?.requestSchema, {
      requestId: message.requestId,
      podId: connection.podId,
      service: operation.service,
      operation: operation.operation
    })
    if (!requestValidation.ok) {
      const error = validationError(requestValidation)
      this._emitMetric('request.rejected', null, {
        requestId: message.requestId,
        podId: connection.podId,
        service: operation.service,
        operation: operation.operation,
        operationKey: operation.value,
        outcome: 'error',
        status: 'failed',
        errorCode: error.code
      })
      const failed = this._failed(message.requestId, error.code, error.message, false)
      failed.error = errorPayload(error)
      failed._request = requestEnvelope(
        message.payload,
        message._request,
        registered?.requestSchema?.method,
        { statusCode: null, error: errorPayload(error) }
      )
      return this._send(connection, failed)
    }

    if (!this.queue && this.memoryQueue.length >= this.maxQueueSize) {
      this._emitMetric('request.rejected', null, {
        requestId: message.requestId,
        podId: connection.podId,
        service: operation.service,
        operation: operation.operation,
        operationKey: operation.value,
        outcome: 'error',
        status: 'failed',
        errorCode: 'QUEUE_FULL'
      })
      return this._send(connection, this._failed(message.requestId, 'QUEUE_FULL', 'Gateway queue is full', true))
    }

    const idempotencyKey = message.idempotencyKey
    const dedupKey = idempotencyKey ? `${connection.podId}:${idempotencyKey}` : null
    if (dedupKey) {
      let duplicate
      try {
        // The in-memory fallback has to claim synchronously.  Calling an
        // async helper here would yield before the first request reserves its
        // key, allowing a concurrent duplicate to enter the queue as well.
        duplicate = this.queue
          ? await this._claimIdempotency(connection.podId, idempotencyKey, message.requestId, dedupKey)
          : this._claimIdempotencySync(dedupKey)
      } catch (error) {
        this._emitMetric('request.rejected', null, {
          requestId: message.requestId,
          podId: connection.podId,
          service: operation.service,
          operation: operation.operation,
          operationKey: operation.value,
          outcome: 'error',
          status: 'failed',
          errorCode: 'IDEMPOTENCY_UNAVAILABLE'
        })
        return this._send(connection, this._failed(message.requestId, 'IDEMPOTENCY_UNAVAILABLE', error.message, true))
      }
      if (duplicate) {
        this._emitMetric('request.duplicate', null, {
          requestId: message.requestId,
          podId: connection.podId,
          service: operation.service,
          operation: operation.operation,
          operationKey: operation.value,
          duplicateOf: duplicate.requestId || ''
        })
        if (duplicate.response) return this._send(connection, this._responseForRequest(duplicate.response, message.requestId))
        return this._send(connection, { type: 'accepted', requestId: message.requestId, duplicateOf: duplicate.requestId })
      }
    }

    const receivedAt = Date.now()
    const enqueuedAt = Date.now()
    const item = {
      connectionId: connection.connectionId,
      podId: connection.podId,
      instanceId: connection.instanceId,
      request: { ...message, _request: requestValidation.request },
      operation: registeredOperation,
      receivedAt,
      enqueuedAt,
      lastQueuedAt: enqueuedAt,
      queueWaitMs: 0,
      rateLimitWaitMs: 0,
      upstreamMs: 0,
      attempts: 0,
      dedupKey
    }
    this._emitMetric('request.received', item, { type: message.type, priority: message.priority || 'normal' })
    this.logger.debug?.(`[gateway] enqueue request=${message.requestId} operation=${operation.value} pod=${connection.podId}`)
    if (dedupKey) this.inflightIdempotency.set(dedupKey, item)
    let queueEntry
    try {
      if (this.queue) {
        queueEntry = await this.queue.enqueue(item)
      } else {
        this.memoryQueue.push(item)
        this._sortQueue()
      }
    } catch (error) {
      if (dedupKey) {
        this.inflightIdempotency.delete(dedupKey)
        await this._releaseIdempotency(connection.podId, idempotencyKey, message.requestId).catch(() => {})
      }
      this._emitMetric('request.rejected', item, {
        outcome: 'error',
        status: 'failed',
        errorCode: error.code || 'QUEUE_UNAVAILABLE'
      })
      return this._send(connection, this._failed(message.requestId, error.code || 'QUEUE_UNAVAILABLE', error.message, true))
    }
    this.logger.debug?.(`[gateway] accepted request=${message.requestId}${queueEntry?.id ? ` queueId=${queueEntry.id}` : ''}`)
    item.acceptedAt = Date.now()
    this._emitMetric('request.accepted', item, {
      queueId: queueEntry?.id || '',
      queueStream: queueEntry?.stream || '',
      priority: queueEntry?.priority || message.priority || 'normal'
    })
    this._send(connection, {
      type: 'accepted',
      requestId: message.requestId,
      ...(queueEntry?.id ? { queueId: queueEntry.id } : {}),
      ...(!this.queue ? { queuePosition: this.memoryQueue.indexOf(item) + 1 } : {})
    })
    if (!this.queue) this._pump()
  }

  _pump () {
    if (this.queue || this.pumping || this.pumpTimer || !this.memoryQueue.length) return
    this.pumping = true
    ;(async () => {
      try {
        while (this.active < this.concurrency && this.memoryQueue.length) {
          const item = this.memoryQueue.shift()
          this._markProcessing(item)
          if (this._expired(item)) {
            await this._finish(item, null, this._error('DEADLINE_EXCEEDED', 'Task deadline exceeded before execution', true))
            continue
          }
          let decision
          try {
            decision = this.rateLimiter
              ? await this.rateLimiter.allow(item.operation.service, 1, item.operation.value, item.operation.rateLimit)
              : { allowed: true, retryAfterMs: 0 }
          } catch (error) {
            await this._finish(item, null, this._error('RATE_LIMITER_UNAVAILABLE', 'Global rate limiter is unavailable', true))
            continue
          }
          this._emitMetric('rate_limit.decision', item, {
            allowed: Boolean(decision.allowed),
            retryAfterMs: Number(decision.retryAfterMs || 0)
          })
          if (!decision.allowed) {
            item.rateLimitBlockedAt = Date.now()
            item.lastQueuedAt = Date.now()
            this.memoryQueue.unshift(item)
            this._schedulePump(decision.retryAfterMs || 100)
            break
          }
          this.active++
          this._execute(item).finally(() => {
            this.active--
            this._pump()
          })
        }
      } finally {
        this.pumping = false
      }
    })().catch(error => this.logger.error?.('[gateway] pump error', error))
  }

  async _execute (item) {
    item.attempts++
    const configuredTimeoutMs = Number(item.request.upstreamTimeoutMs || this.upstreamTimeoutMs)
    // Leave a small response-delivery window before the POD-side deadline.
    // Without this cushion both timers fire at 60s and the caller reports its
    // generic DEADLINE_EXCEEDED before the gateway can return UPSTREAM_TIMEOUT.
    const deadlineMs = item.request.deadlineAt ? Date.parse(item.request.deadlineAt) - Date.now() : Infinity
    const timeoutMs = Math.max(1, Math.min(
      configuredTimeoutMs,
      Number.isFinite(deadlineMs) ? Math.max(1, deadlineMs - 250) : configuredTimeoutMs
    ))
    this._checkClaimIdleMargin(item, timeoutMs)
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)
    const upstreamStartedAt = process.hrtime.bigint()
    const requestContext = {
      requestId: item.request.requestId,
      podId: item.podId,
      service: item.operation.service,
      operation: item.operation.operation,
      method: item.operation.requestSchema?.method,
      signal: controller.signal,
      deadlineAt: item.request.deadlineAt,
      idempotencyKey: item.request.idempotencyKey || item.request.requestId,
      attempt: item.attempts,
      request: normalizeRequest(item.request._request)
    }
    this._emitMetric('upstream.started', item, { attempt: item.attempts })
    this.logger.debug?.(`[gateway] execute request=${item.request.requestId} operation=${item.operation.value} timeoutMs=${timeoutMs}`)
    try {
      const adapter = this.adapters.get(item.operation.value)
      const execute = typeof adapter === 'function' ? adapter : adapter?.execute
      if (typeof execute !== 'function') throw this._error('ADAPTER_INVALID', `Adapter for ${item.operation.value} is invalid`)
      const result = await execute(item.request.payload, requestContext)
      this._recordUpstream(item, Number(process.hrtime.bigint() - upstreamStartedAt) / 1e6, 'success', null, requestContext.response)
      await this._finish(item, result, null, requestContext.response)
    } catch (error) {
      this._recordUpstream(item, Number(process.hrtime.bigint() - upstreamStartedAt) / 1e6, 'error', error, requestContext.response)
      const normalized = this._error(error.code || 'UPSTREAM_ERROR', error.message || 'Upstream request failed', Boolean(error.retryable), error.retryAfterMs)
      if (error.status != null) normalized.status = Number(error.status)
      if (normalized.status == null && requestContext.response?.statusCode != null) normalized.status = Number(requestContext.response.statusCode)
      if (error.responseBody !== undefined) normalized.responseBody = error.responseBody
      if (normalized.retryable && item.attempts <= this.maxAttempts && !this._expired(item)) {
        const wait = Number(normalized.retryAfterMs || Math.min(1000 * item.attempts, 10000))
        if (typeof this.queue?.scheduleRetry === 'function') {
          try {
            await this._requeue(item, wait, { preserveDepth: true })
            return { deferred: true }
          } catch (requeueError) {
            await this._finish(item, null, this._error('QUEUE_UNAVAILABLE', requeueError.message, true))
          }
        } else {
          setTimeout(() => {
            this._requeue(item, wait).catch(error => this._finish(item, null, this._error('QUEUE_UNAVAILABLE', error.message, true)).catch(() => {}))
          }, wait)
        }
      } else {
        await this._finish(item, null, normalized, requestContext.response)
      }
    } finally {
      this.logger.debug?.(`[gateway] finished request=${item.request.requestId}`)
      clearTimeout(timeout)
    }
  }

  // A Redis Stream entry is reclaimed via XAUTOCLAIM once it has sat
  // unacknowledged for claimIdleMs. If some operation's own upstream timeout
  // is longer than that, a slow-but-healthy call can be reclaimed and
  // dispatched a second time while the first attempt is still running. Warn
  // once per operation so this misconfiguration surfaces in logs instead of
  // being discovered as an intermittent duplicate-dispatch report.
  _checkClaimIdleMargin (item, timeoutMs) {
    const claimIdleMs = this.queue?.claimIdleMs
    if (!claimIdleMs || timeoutMs <= claimIdleMs) return
    const operationKey = item.operation?.value || 'unknown'
    if (this._claimIdleWarned.has(operationKey)) return
    this._claimIdleWarned.add(operationKey)
    this.logger.warn?.(
      `[gateway] operation=${operationKey} upstream timeout ${timeoutMs}ms exceeds the Redis Stream claimIdleMs ${claimIdleMs}ms; ` +
      'a slow-but-healthy call can be reclaimed and re-dispatched while still running. Raise claimIdleMs or lower this operation\'s timeout.'
    )
  }

  async _handleQueuedItem (item, _queueMeta) {
    this.active++
    this._markProcessing(item)
    this.logger.debug?.(`processing request=${item.request.requestId} operation=${item.operation.value} queueId=${item.queueId || ''}`)
    try {
      if (this.queue?.getIdempotencyResult && item.request.idempotencyKey) {
        const cached = await this.queue.getIdempotencyResult(item.podId, item.request.idempotencyKey)
        if (cached?.response) {
          await this._deliverResponse(item, this._responseForRequest(cached.response, item.request.requestId))
          return
        }
      }
      if (this._expired(item)) {
        await this._finish(item, null, this._error('DEADLINE_EXCEEDED', 'Task deadline exceeded before execution', true))
        return
      }
      let decision
      try {
        decision = this.rateLimiter
          ? await this.rateLimiter.allow(item.operation.service, 1, item.operation.value, item.operation.rateLimit)
          : { allowed: true, retryAfterMs: 0 }
      } catch (error) {
        this.logger.error?.(`rate limiter failed request=${item.request.requestId}: ${error.message}`)
        await this._finish(item, null, this._error('RATE_LIMITER_UNAVAILABLE', 'Global rate limiter is unavailable', true))
        return
      }
      this._emitMetric('rate_limit.decision', item, {
        allowed: Boolean(decision.allowed),
        retryAfterMs: Number(decision.retryAfterMs || 0)
      })
      if (!decision.allowed) {
        item.rateLimitBlockedAt = Date.now()
        this.logger.debug?.(`rate limited request=${item.request.requestId} retryAfterMs=${decision.retryAfterMs || 0}`)
        const retryAfterMs = Math.max(1, Number(decision.retryAfterMs || 100))
        if (typeof this.queue?.scheduleRetry === 'function') {
          try {
            // The current Stream entry is still counted by Redis. Transfer
            // that depth slot to the delayed retry and let _dispatch ACK the
            // original entry without decrementing it.
            await this._requeue(item, retryAfterMs, { preserveDepth: true })
            return { deferred: true }
          } catch (error) {
            await this._finish(item, null, this._error('QUEUE_UNAVAILABLE', error.message, true))
            return
          }
        }
        setTimeout(() => {
          this._requeue(item).catch(error => this._finish(item, null, this._error('QUEUE_UNAVAILABLE', error.message, true)).catch(() => {}))
        }, retryAfterMs)
        return
      }
      return await this._execute(item)
    } finally {
      this.active--
    }
  }

  async _requeue (item, delayMs = 0, options = {}) {
    this.logger.debug?.(`requeue request=${item.request.requestId} operation=${item.operation.value}`)
    item.lastQueuedAt = Date.now()
    if (this.queue) {
      if (typeof this.queue.scheduleRetry === 'function') return this.queue.scheduleRetry(item, delayMs, options)
      return this.queue.enqueue(item)
    }
    this.memoryQueue.push(item)
    this._sortQueue()
    this._pump()
  }

  async _finish (item, result, error, responseMeta = {}) {
    const wallTotalMs = Math.max(0, Date.now() - Number(item.receivedAt || item.enqueuedAt || Date.now()))
    // Date.now() has millisecond resolution while upstream timings use a
    // monotonic high-resolution clock. Keep the reported total coherent when
    // clock quantisation makes the wall-clock value a fraction smaller.
    const measuredTotalMs = Number(item.queueWaitMs || 0) +
      Number(item.rateLimitWaitMs || 0) + Number(item.upstreamMs || 0)
    const totalMs = Math.max(wallTotalMs, measuredTotalMs)
    const statusCode = responseMeta?.statusCode != null
      ? Number(responseMeta.statusCode)
      : (error?.status != null ? Number(error.status) : null)
    const responseBody = error?.responseBody !== undefined ? error.responseBody : result
    const requestOutput = {
      statusCode,
      ...(error
        ? {
            error: errorPayload(error),
            ...(error.responseBody !== undefined ? { body: responseBody } : {})
          }
        : { body: responseBody })
    }
    const response = error
      ? {
          type: 'result',
          requestId: item.request.requestId,
          operation: item.operation.value,
          ok: false,
          durationMs: totalMs,
          error: errorPayload(error),
          response: { statusCode },
          _request: requestEnvelope(
            item.request.payload,
            item.request._request,
            item.operation.requestSchema?.method,
            requestOutput
          )
        }
      : {
          type: 'result',
          requestId: item.request.requestId,
          operation: item.operation.value,
          ok: true,
          payload: result,
          durationMs: totalMs,
          response: { statusCode },
          _request: requestEnvelope(
            item.request.payload,
            item.request._request,
            item.operation.requestSchema?.method,
            requestOutput
          )
        }
    if (item.dedupKey) {
      this.inflightIdempotency.delete(item.dedupKey)
      this.idempotencyResults.set(item.dedupKey, response)
      // The POD response must not wait for a secondary Redis write. If that
      // connection is degraded, waiting here would make a completed upstream
      // request look like a Gateway timeout. Persist after delivery.
      if (this.queue) {
        this.queue.storeIdempotencyResult(
          item.podId,
          item.request.idempotencyKey,
          response
        ).catch(error => this.logger.warn?.('[gateway] idempotency result storage failed', error.message))
      }
      else setTimeout(() => this.idempotencyResults.delete(item.dedupKey), this.resultTtlSeconds * 1000).unref?.()
    }
    this.logger.debug?.(`finish request=${item.request.requestId} ok=${!error} code=${error?.code || ''}`)
    const deliveryStartedAt = process.hrtime.bigint()
    const delivery = await this._deliverResponse(item, response)
    const deliveryMs = Number(process.hrtime.bigint() - deliveryStartedAt) / 1e6
    this._emitMetric('request.completed', item, {
      outcome: error ? 'error' : 'success',
      status: error ? 'failed' : 'completed',
      durationMs: totalMs,
      delivered: delivery.delivered,
      errorCode: error?.code || '',
      httpStatus: statusCode || 0,
      timings: {
        queueMs: Number(item.queueWaitMs || 0),
        rateLimitMs: Number(item.rateLimitWaitMs || 0),
        upstreamMs: Number(item.upstreamMs || 0),
        deliveryMs,
        totalMs
      }
    })
  }

  async _deliverResponse (item, response) {
    const connection = this._findConnection(item)
    // A connection object can exist briefly while its socket is already
    // closing. Do not ACK and drop the result in that case: persist it and
    // replay it when the POD reconnects.
    const delivered = connection && this._send(connection, response)
    this.logger.info?.(`[gateway] response request=${item.request.requestId} delivered=${Boolean(delivered)} pod=${item.podId}`)
    if (!delivered) await this._storePending(item.podId, item.request.requestId, response)
    return { delivered: Boolean(delivered) }
  }

  _markProcessing (item) {
    const now = Date.now()
    const queuedAt = Number(item.lastQueuedAt || item.enqueuedAt || now)
    item.queueWaitMs = Number(item.queueWaitMs || 0) + Math.max(0, now - queuedAt)
    if (item.rateLimitBlockedAt) {
      item.rateLimitWaitMs = Number(item.rateLimitWaitMs || 0) + Math.max(0, now - item.rateLimitBlockedAt)
      delete item.rateLimitBlockedAt
    }
    item.lastQueuedAt = now
    this._emitMetric('request.processing', item, {
      queueWaitMs: Number(item.queueWaitMs || 0),
      rateLimitMs: Number(item.rateLimitWaitMs || 0)
    })
  }

  _recordUpstream (item, durationMs, outcome, error, responseMeta = {}) {
    const elapsed = Math.max(0, Number(durationMs) || 0)
    item.upstreamMs = Number(item.upstreamMs || 0) + elapsed
    item.lastUpstreamMs = elapsed
    this._emitMetric('upstream.completed', item, {
      attempt: item.attempts,
      durationMs: elapsed,
      outcome,
      errorCode: error?.code || '',
      httpStatus: Number(responseMeta?.statusCode || error?.status || 0)
    })
  }

  _emitMetric (event, item, extra = {}) {
    const operation = item?.operation
    const request = item?.request
    const metric = {
      event,
      timestamp: new Date().toISOString(),
      requestId: extra.requestId ?? request?.requestId ?? '',
      podId: extra.podId ?? item?.podId ?? '',
      service: extra.service ?? operation?.service ?? '',
      operation: extra.operation ?? operation?.operation ?? '',
      operationKey: extra.operationKey ?? operation?.value ?? '',
      ...extra
    }
    try {
      this.emit('metric', metric)
    } catch (error) {
      this.logger.warn?.(`[gateway] metric listener failed: ${error.message}`)
    }
    return metric
  }

  async _claimIdempotency (podId, idempotencyKey, requestId, dedupKey) {
    if (this.queue?.claimIdempotency) {
      const claim = await this.queue.claimIdempotency(podId, idempotencyKey, requestId)
      // A successful claim is not a duplicate. The Redis queue returns an
      // explicit { claimed: true } marker; normalize it to null so the
      // enqueue path continues to XADD the new item.
      return claim?.claimed === false ? claim : null
    }
    return this._claimIdempotencySync(dedupKey)
  }

  _claimIdempotencySync (dedupKey) {
    if (this.inflightIdempotency.has(dedupKey)) {
      return { requestId: this.inflightIdempotency.get(dedupKey).request.requestId }
    }
    if (this.idempotencyResults.has(dedupKey)) {
      return { requestId: this.idempotencyResults.get(dedupKey).requestId, response: this.idempotencyResults.get(dedupKey) }
    }
    return null
  }

  async _releaseIdempotency (podId, idempotencyKey, requestId) {
    if (this.queue?.releaseIdempotency) await this.queue.releaseIdempotency(podId, idempotencyKey, requestId)
  }

  _responseForRequest (response, requestId) {
    if (response.requestId === requestId) return response
    return { ...response, requestId, duplicateOf: response.requestId }
  }

  _findConnection (item) {
    // Prefer the originating socket. Falling back to an arbitrary connection
    // with the same podId can deliver a result to a stale WebSocket after a
    // reconnect or when two flows accidentally share a podId.
    for (const connection of this.connections.values()) {
      if (connection.authenticated && connection.connectionId === item.connectionId) return connection
    }
    const current = this.connectionsByPod.get(item.podId)
    if (current && current.authenticated) return current
    return null
  }

  async _storePending (podId, requestId, response) {
    if (this.queue?.storePendingResult) {
      await this.queue.storePendingResult(podId, response)
      return
    }
    if (!this.pendingResults.has(podId)) this.pendingResults.set(podId, new Map())
    this.pendingResults.get(podId).set(requestId, response)
  }

  async _replayPending (connection) {
    const pending = this.pendingResults.get(connection.podId)
    if (pending) {
      for (const response of pending.values()) this._send(connection, response)
      this.pendingResults.delete(connection.podId)
    }
    if (this.queue?.takePendingResults) {
      const responses = await this.queue.takePendingResults(connection.podId)
      for (const response of responses) this._send(connection, response)
    }
  }

  _disconnect (connection) {
    this.logger.debug?.(`ws connection closed connection=${connection.connectionId} pod=${connection.podId || ''}`)
    this.connections.delete(connection.ws)
    if (this.connectionsByPod.get(connection.podId) === connection) this.connectionsByPod.delete(connection.podId)
  }

  _schedulePump (delayMs) {
    if (this.pumpTimer) return
    this.pumpTimer = setTimeout(() => {
      this.pumpTimer = null
      this._pump()
    }, Math.max(1, Number(delayMs)))
  }

  _sortQueue () {
    this.memoryQueue.sort((left, right) => {
      const priority = (PRIORITY[right.request.priority] || PRIORITY.normal) - (PRIORITY[left.request.priority] || PRIORITY.normal)
      return priority || left.enqueuedAt - right.enqueuedAt
    })
  }

  _expired (item) {
    return item.request.deadlineAt && Date.parse(item.request.deadlineAt) <= Date.now()
  }

  _send (connection, message) {
    if (!connection?.ws || connection.ws.readyState !== WebSocket.OPEN) return false
    try {
      connection.ws.send(JSON.stringify(message))
      return true
    } catch { return false }
  }

  _failed (requestId, code, message, retryable = false) {
    return {
      type: 'result',
      requestId,
      ok: false,
      error: { code, message, retryable }
    }
  }

  _error (code, message, retryable = false, retryAfterMs) {
    const error = new Error(message)
    error.code = code
    error.retryable = retryable
    if (retryAfterMs != null) error.retryAfterMs = retryAfterMs
    return error
  }

  _health (request, response) {
    if (request.url !== '/healthz' && request.url !== '/readyz') {
      response.writeHead(404, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ error: 'not_found' }))
      return
    }
    const rateLimiterReady = this.rateLimiter == null || this.rateLimiter.client?.isReady !== false
    const queueReady = this.queue == null || (typeof this.queue.isReady === 'function' ? this.queue.isReady() : true)
    const ready = this.started && rateLimiterReady && queueReady
    response.writeHead(ready ? 200 : 503, { 'content-type': 'application/json' })
    response.end(JSON.stringify({
      ok: ready,
      queueDepth: this.queue ? null : this.memoryQueue.length,
      active: this.active,
      ...(this.rateLimiter ? { rateLimiterReady } : {}),
      ...(this.queue ? { queueReady } : {})
    }))
  }
}

function positiveInteger (value, fallback) {
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback
}

module.exports = { GatewayServer }
