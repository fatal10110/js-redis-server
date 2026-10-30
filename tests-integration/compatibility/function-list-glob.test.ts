import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * FUNCTION LIST LIBRARYNAME is a glob, matched without regard to case
 * (`stringmatchlen(..., 1)`, redis@7.0.15 functions.c:543, #583). Checked
 * against redis-server 7.0.15, 8.0.0 and valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile

describe(`FUNCTION LIST LIBRARYNAME glob (${testRunner.getBackendName()}, ${profile})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  test(
    'matches library names as a case-insensitive glob',
    {
      skip: profile === 'redis-6.2',
    },
    async () => {
      const conn = await RawRedisConnection.connect('127.0.0.1', port)
      connections.push(conn)
      // Lower-case letters and digits only: no glob characters of its own.
      const id = randomKey()
      const library = `gl${id}lib`
      const fn = `f${id}`
      await expectReply(
        conn,
        [
          'FUNCTION',
          'LOAD',
          `#!lua name=${library}\nredis.register_function('${fn}', function() return 1 end)`,
        ],
        `$${library.length}\r\n${library}\r\n`,
      )

      const listed =
        `*1\r\n*6\r\n$12\r\nlibrary_name\r\n$${library.length}\r\n${library}\r\n` +
        '$6\r\nengine\r\n$3\r\nLUA\r\n$9\r\nfunctions\r\n' +
        `*1\r\n*6\r\n$4\r\nname\r\n$${fn.length}\r\n${fn}\r\n` +
        '$11\r\ndescription\r\n$-1\r\n$5\r\nflags\r\n*0\r\n'

      try {
        for (const pattern of [
          library,
          `gl${id}*`,
          `gl${id}l?b`,
          `[fg]l${id}lib`,
          `[e-h]l${id}lib`,
          `\\gl${id}lib`,
          `GL${id.toUpperCase()}*`,
          library.toUpperCase(),
          `[E-H]L${id}LIB`,
        ]) {
          await expectReply(
            conn,
            ['FUNCTION', 'LIST', 'LIBRARYNAME', pattern],
            listed,
          )
        }

        for (const pattern of [`${id}lib`, `gl${id}`, `[^g]l${id}lib`]) {
          await expectReply(
            conn,
            ['FUNCTION', 'LIST', 'LIBRARYNAME', pattern],
            '*0\r\n',
          )
        }
      } finally {
        await expectReply(conn, ['FUNCTION', 'DELETE', library], '+OK\r\n')
      }
    },
  )
})
