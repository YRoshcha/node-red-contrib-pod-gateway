'use strict'

/**
 * Create a small logger adapter that works with both Node-RED's RED.log and
 * the native console. Verbose messages are intentionally routed through the
 * normal info channel when enabled so they are visible with Node-RED's
 * default log level.
 */
function createLogger (baseLogger = console, options = {}) {
  const verbose = options.verbose === true || options.verbose === 'true' || options.verbose === 1 || options.verbose === '1'
  const sink = baseLogger || console

  const write = (level, args) => {
    const method = typeof sink[level] === 'function'
      ? sink[level]
      : (typeof sink.info === 'function' ? sink.info : console.log)
    // RED.log methods accept one message argument. Passing several arguments
    // silently drops everything after the first one, which made trace lines
    // appear as just "[pod-gateway][trace]" in Node-RED. Serialize all parts
    // into one safe line and avoid logging payloads/secrets by construction.
    const message = args.map(value => {
      if (typeof value === 'string') return value
      if (value instanceof Error) return `${value.name}: ${value.message}`
      if (value === undefined) return 'undefined'
      try { return JSON.stringify(value) } catch { return String(value) }
    }).join(' ')
    method.call(sink, message)
  }

  return {
    verbose,
    debug: (...args) => {
      if (verbose) write('info', ['[pod-gateway][trace]', ...args])
    },
    info: (...args) => write('info', args),
    warn: (...args) => write('warn', args),
    error: (...args) => write('error', args)
  }
}

module.exports = { createLogger }
