'use strict'

const fs = require('node:fs')
const path = require('node:path')

function loadDotEnv (file = process.env.POD_GATEWAY_ENV_FILE || path.join(process.cwd(), '.env')) {
  if (!fs.existsSync(file)) return false
  const content = fs.readFileSync(file, 'utf8')
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const match = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/)
    if (!match || Object.prototype.hasOwnProperty.call(process.env, match[1])) continue
    let value = match[2].trim()
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1)
    }
    process.env[match[1]] = value
  }
  return true
}

module.exports = { loadDotEnv }
