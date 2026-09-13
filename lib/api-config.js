'use strict'

function parseHeaders (value) {
  if (!value) return {}
  const parsed = typeof value === 'object' ? value : JSON.parse(value)
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('Headers must be a JSON object')
  }
  return expandEnvironment(parsed)
}

function expandEnvironment (value) {
  if (typeof value === 'string') {
    return value.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_match, name) => process.env[name] || '')
  }
  if (Array.isArray(value)) return value.map(expandEnvironment)
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, expandEnvironment(item)]))
  }
  return value
}

function joinUrl (baseUrl, path) {
  const base = String(baseUrl || '').trim()
  if (!base) throw new Error('API base URL is required')
  try { new URL(base) } catch { throw new Error(`Invalid API base URL: ${base}`) }
  if (!path) return base
  return `${base.replace(/\/+$/, '')}/${String(path).replace(/^\/+/, '')}`
}

function requestOptions ({ baseUrl, headers, apiKeyHeader, apiKeyPrefix, apiKey }, path, extraHeaders = {}) {
  const mergedHeaders = { ...(headers || {}) }
  if (apiKeyHeader && apiKey) mergedHeaders[apiKeyHeader] = `${apiKeyPrefix || ''}${apiKey}`
  return {
    url: joinUrl(baseUrl, path),
    headers: { ...mergedHeaders, ...extraHeaders }
  }
}

module.exports = { parseHeaders, expandEnvironment, joinUrl, requestOptions }
