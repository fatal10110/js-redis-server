import { createInMemoryClient } from '../../src/in-memory-client'

const el = document.getElementById('log') as HTMLDivElement
const lines: string[] = []
const log = (msg: string) => {
  lines.push(msg)
  el.textContent = lines.join('\n')
}

async function run() {
  lines.length = 0
  const client = await createInMemoryClient()

  await client.command('SET', 'hello', 'world')
  log('SET hello world')
  log('GET hello -> ' + (await client.command('GET', 'hello')))

  await client.command('HSET', 'h', 'f1', 'a', 'f2', 'b')
  log('HSET h f1 a f2 b')
  log('HGETALL h -> ' + JSON.stringify(await client.command('HGETALL', 'h')))

  await client.command('RPUSH', 'list', 'x', 'y', 'z')
  log('RPUSH list x y z')
  log(
    'LRANGE list 0 -1 -> ' +
      JSON.stringify(await client.command('LRANGE', 'list', '0', '-1')),
  )

  // Lua — exercises the vendored browser-loadable lua-redis-wasm build.
  log('\n-- Lua --')
  const script = "return redis.call('GET', KEYS[1])"
  log(
    'EVAL "return redis.call(\'GET\', KEYS[1])" 1 hello -> ' +
      (await client.command('EVAL', script, '1', 'hello')),
  )
  const sha = (await client.command('SCRIPT', 'LOAD', script)) as string
  log('SCRIPT LOAD -> ' + sha)
  log(
    'EVALSHA <sha> 1 hello -> ' +
      (await client.command('EVALSHA', sha, '1', 'hello')),
  )
  log(
    'EVAL "return 1+1" 0 -> ' +
      (await client.command('EVAL', 'return 1+1', '0')),
  )

  log('\nOK — full pipeline incl. Lua works in the browser.')

  client.close()
}

run().catch(err => log('ERROR: ' + (err?.stack ?? err)))
