import { Resp2Transport } from './core/transports/resp2'
import { createCustomCommander } from './commanders/custom/commander'
import { ClusterNetwork } from './core/cluster/network'
import type { Logger } from './types'

type Mode = 'single' | 'cluster'

export type CliOptions = {
  mode: Mode
  port: number
  masters: number
  slaves: number
  basePort: number
  help: boolean
  debug: boolean
}

const defaultOptions: CliOptions = {
  mode: 'single',
  port: 6379,
  masters: 3,
  slaves: 0,
  basePort: 30000,
  help: false,
  debug: false,
}

export function parseArgs(args: string[]): CliOptions {
  const options: CliOptions = { ...defaultOptions }

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]

    if (arg === '--help' || arg === '-h') {
      options.help = true
      continue
    }

    if (arg === '--debug' || arg === '-d') {
      options.debug = true
      continue
    }

    if (arg === '--cluster') {
      options.mode = 'cluster'
      continue
    }

    if (arg === '--single') {
      options.mode = 'single'
      continue
    }

    if (arg === '--mode') {
      const value = args[++i]
      if (value !== 'single' && value !== 'cluster') {
        throw new Error(`Invalid mode "${value}"`)
      }
      options.mode = value
      continue
    }

    if (arg === '--port') {
      options.port = parseInteger(args[++i], '--port')
      continue
    }

    if (arg === '--base-port') {
      options.basePort = parseInteger(args[++i], '--base-port')
      continue
    }

    if (arg === '--masters') {
      options.masters = parseInteger(args[++i], '--masters')
      continue
    }

    if (arg === '--slaves') {
      options.slaves = parseInteger(args[++i], '--slaves')
      continue
    }

    throw new Error(`Unknown argument "${arg}"`)
  }

  return options
}

function parseInteger(value: string | undefined, name: string): number {
  if (!value) {
    throw new Error(`Missing value for ${name}`)
  }
  const parsed = Number(value)
  if (!Number.isInteger(parsed)) {
    throw new Error(`Invalid value for ${name}: ${value}`)
  }
  return parsed
}

function validatePort(port: number, name: string): void {
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`Invalid ${name} ${port}`)
  }
}

function createLogger(debug = false): Logger {
  return {
    info: (msg, metadata) => {
      if (metadata) {
        console.log(msg, metadata)
      } else {
        console.log(msg)
      }
    },
    error: (msg, metadata) => {
      if (metadata) {
        console.error(msg, metadata)
      } else {
        console.error(msg)
      }
    },
    debug: (msg, metadata) => {
      if (debug) {
        if (metadata) {
          console.debug(msg, metadata)
        } else {
          console.debug(msg)
        }
      }
    },
  }
}

function printHelp() {
  console.log(`Usage: js-redis-server [options]

Modes:
  --single               Run a single Redis server (default)
  --cluster              Run a Redis cluster
  --mode <single|cluster>

Single server options:
  --port <number>        Port to listen on (default 6379)

Cluster options:
  --masters <number>     Number of masters (default 3)
  --slaves <number>      Number of replicas per master (default 0)
  --base-port <number>   Starting port for cluster nodes (default 30000)

General:
  -d, --debug            Enable debug logging
  -h, --help             Show help
`)
}

async function runSingle(options: CliOptions, logger: Logger) {
  validatePort(options.port, 'port')
  logger.debug('Starting single server mode', { port: options.port })
  const commanderFactory = await createCustomCommander(logger)
  const transport = new Resp2Transport(
    logger,
    commanderFactory.createCommander(),
  )

  await transport.listen(options.port)
  const address = transport.getAddress()
  logger.info(`Single Redis server listening at ${address}`)

  const shutdown = async () => {
    await transport.close()
    await commanderFactory.shutdown()
  }

  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
}

async function runCluster(options: CliOptions, logger: Logger) {
  validatePort(options.basePort, 'base-port')
  if (!Number.isInteger(options.masters) || options.masters < 1) {
    throw new Error(`Invalid masters count ${options.masters}`)
  }
  if (!Number.isInteger(options.slaves) || options.slaves < 0) {
    throw new Error(`Invalid slaves count ${options.slaves}`)
  }
  const totalNodes = options.masters * (options.slaves + 1)
  if (options.basePort + totalNodes - 1 > 65535) {
    throw new Error('Cluster base-port range exceeds 65535')
  }

  logger.debug('Starting cluster mode', {
    masters: options.masters,
    slaves: options.slaves,
    basePort: options.basePort,
    totalNodes,
  })

  const network = new ClusterNetwork(logger)
  await network.init({
    masters: options.masters,
    slaves: options.slaves,
    basePort: options.basePort,
  })

  const nodes = network
    .getAll()
    .map(n => `${n.id} ${n.host}:${n.port}`)
    .join(', ')
  logger.info(`Cluster nodes: ${nodes}`)

  const shutdown = async () => {
    await network.shutdown()
  }

  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
}

export async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv)
  if (options.help) {
    printHelp()
    return
  }

  const logger = createLogger(options.debug)

  if (options.mode === 'cluster') {
    await runCluster(options, logger)
  } else {
    await runSingle(options, logger)
  }
}

// Run when executed directly as CLI (ESM check)
// For bin scripts, we check if this is the main module by comparing resolved paths
import { fileURLToPath } from 'node:url'
import { realpathSync } from 'node:fs'
import { resolve } from 'node:path'

// When used as a bin script (e.g., via npm link or global install),
// process.argv[1] might be a symlink, so we resolve both paths
const scriptPath = resolve(realpathSync(process.argv[1] || ''))
const modulePath = resolve(fileURLToPath(import.meta.url))
const isMain = scriptPath === modulePath

if (isMain) {
  main().catch(err => {
    console.error(err)
    process.exitCode = 1
  })
}
