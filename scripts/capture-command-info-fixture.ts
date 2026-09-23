/**
 * Regenerates `tests/fixtures/command-info-redis-8.0.json`: the arity and
 * legacy first/last/step key positions real Redis 8.0 reports in
 * `COMMAND INFO` for every command and subcommand the mock registers at its
 * default (`redis-8.0`) profile (#370).
 *
 * Only `COMMAND INFO` is sent, so any Redis 8.0 server will do, e.g.
 *
 *   docker run -d --rm --name cmdinfo-80 -p 47480:6379 redis:8.0
 *   node --import tsx scripts/capture-command-info-fixture.ts 47480
 *
 * The script fails if the server does not know a command the mock registers.
 */
import { writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import Redis from 'ioredis'
import { createRedisCommandExecutor } from '../src/internal'

type CommandInfoReply = [string, number, unknown, number, number, number]

const port = Number(process.argv[2])
if (!Number.isInteger(port)) {
  console.error('usage: capture-command-info-fixture.ts <port>')
  process.exit(1)
}

const OUTPUT = resolve(
  __dirname,
  '../tests/fixtures/command-info-redis-8.0.json',
)

async function main(): Promise<void> {
  const names: string[] = []
  for (const definition of createRedisCommandExecutor().getCommandDefinitions()) {
    names.push(definition.name)
    for (const subcommand of definition.introspection?.subcommands ?? []) {
      if (subcommand.name) {
        names.push(subcommand.name)
      }
    }
  }

  const redis = new Redis({ port })
  try {
    const infos = (await redis.command(
      'INFO',
      ...names,
    )) as (CommandInfoReply | null)[]
    const entries: string[] = []
    for (const [i, name] of [...names.entries()].sort(([, a], [, b]) =>
      a.localeCompare(b),
    )) {
      const info = infos[i]
      if (!info) {
        throw new Error(`the server does not know ${name}`)
      }

      const layout = [info[1], info[3], info[4], info[5]]
      entries.push(`  ${JSON.stringify(name)}: [${layout.join(', ')}]`)
    }

    writeFileSync(OUTPUT, `{\n${entries.join(',\n')}\n}\n`)
    console.log(`wrote ${entries.length} commands to ${OUTPUT}`)
  } finally {
    redis.disconnect()
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
