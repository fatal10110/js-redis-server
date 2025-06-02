import { test, describe } from 'node:test'
import assert from 'node:assert'
import { SetCommand } from '../../src/commanders/custom/commands/redis/data/set'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import {
  RedisSyntaxError,
  WrongNumberOfArguments,
  ExpectedInteger,
  InvalidExpireTime,
} from '../../src/core/errors'

describe('SET command', () => {
  test('basic SET operation', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'myvalue')
  })

  test('SET with EX option', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('EX'),
      Buffer.from('10'),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'myvalue')
  })

  test('SET with PX option', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('PX'),
      Buffer.from('5000'),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'myvalue')
  })

  test('SET with EXAT option', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const futureTimestamp = Math.floor(Date.now() / 1000) + 3600 // 1 hour from now

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('EXAT'),
      Buffer.from(futureTimestamp.toString()),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'myvalue')
  })

  test('SET with PXAT option', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const futureTimestamp = Date.now() + 3600000 // 1 hour from now

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('PXAT'),
      Buffer.from(futureTimestamp.toString()),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'myvalue')
  })

  test('SET with NX option - key does not exist', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('NX'),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'myvalue')
  })

  test('SET with NX option - key exists', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    // First set a value
    db.set(Buffer.from('mykey'), new StringDataType(Buffer.from('existing')))

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('NX'),
    ])

    assert.strictEqual(result.response, null)

    // Value should remain unchanged
    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'existing')
  })

  test('SET with XX option - key exists', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    // First set a value
    db.set(Buffer.from('mykey'), new StringDataType(Buffer.from('existing')))

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('XX'),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'myvalue')
  })

  test('SET with XX option - key does not exist', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
      Buffer.from('XX'),
    ])

    assert.strictEqual(result.response, null)

    // Key should not exist
    const storedData = db.get(Buffer.from('mykey'))
    assert.strictEqual(storedData, null)
  })

  test('SET with GET option - key exists', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    // First set a value
    db.set(Buffer.from('mykey'), new StringDataType(Buffer.from('oldvalue')))

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('newvalue'),
      Buffer.from('GET'),
    ])

    assert.ok(Buffer.isBuffer(result.response))
    assert.strictEqual(result.response.toString(), 'oldvalue')

    // New value should be set
    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'newvalue')
  })

  test('SET with GET option - key does not exist', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('newvalue'),
      Buffer.from('GET'),
    ])

    assert.strictEqual(result.response, null)

    // New value should be set
    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'newvalue')
  })

  test('SET with NX and GET options - key exists', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    // First set a value
    db.set(Buffer.from('mykey'), new StringDataType(Buffer.from('existing')))

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('newvalue'),
      Buffer.from('NX'),
      Buffer.from('GET'),
    ])

    assert.ok(Buffer.isBuffer(result.response))
    assert.strictEqual(result.response.toString(), 'existing')

    // Value should remain unchanged
    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'existing')
  })

  test('SET with KEEPTTL option', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    // First set a value with expiration
    const expiration = Date.now() + 10000
    db.set(
      Buffer.from('mykey'),
      new StringDataType(Buffer.from('existing')),
      expiration,
    )

    const result = await setCommand.run(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('newvalue'),
      Buffer.from('KEEPTTL'),
    ])

    assert.strictEqual(result.response, 'OK')

    const storedData = db.get(Buffer.from('mykey'))
    assert.ok(storedData instanceof StringDataType)
    assert.strictEqual(storedData.data.toString(), 'newvalue')
  })

  test('wrong number of arguments', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    // Test with one argument
    try {
      await setCommand.run(Buffer.from('SET'), [Buffer.from('mykey')])
      assert.fail('Expected WrongNumberOfArguments error')
    } catch (err) {
      assert.ok(err instanceof WrongNumberOfArguments)
    }

    // Test with no arguments
    try {
      await setCommand.run(Buffer.from('SET'), [])
      assert.fail('Expected WrongNumberOfArguments error')
    } catch (err) {
      assert.ok(err instanceof WrongNumberOfArguments)
    }
  })

  test('invalid syntax - EX without value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('EX'),
      ])
      assert.fail('Expected RedisSyntaxError')
    } catch (err) {
      assert.ok(err instanceof RedisSyntaxError)
    }
  })

  test('invalid syntax - negative EX value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('EX'),
        Buffer.from('-1'),
      ])
      assert.fail('Expected InvalidExpireTime')
    } catch (err) {
      assert.ok(err instanceof InvalidExpireTime)
    }
  })

  test('invalid type - non-integer EX value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('EX'),
        Buffer.from('abc'),
      ])
      assert.fail('Expected ExpectedInteger')
    } catch (err) {
      assert.ok(err instanceof ExpectedInteger)
    }
  })

  test('invalid syntax - NX and XX together', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('NX'),
        Buffer.from('XX'),
      ])
      assert.fail('Expected RedisSyntaxError')
    } catch (err) {
      assert.ok(err instanceof RedisSyntaxError)
    }
  })

  test('invalid syntax - multiple expiration options', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('EX'),
        Buffer.from('10'),
        Buffer.from('PX'),
        Buffer.from('5000'),
      ])
      assert.fail('Expected RedisSyntaxError')
    } catch (err) {
      assert.ok(err instanceof RedisSyntaxError)
    }
  })

  test('invalid syntax - KEEPTTL with expiration', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('KEEPTTL'),
        Buffer.from('EX'),
        Buffer.from('10'),
      ])
      assert.fail('Expected RedisSyntaxError')
    } catch (err) {
      assert.ok(err instanceof RedisSyntaxError)
    }
  })

  test('invalid syntax - unknown option', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('INVALID'),
      ])
      assert.fail('Expected RedisSyntaxError')
    } catch (err) {
      assert.ok(err instanceof RedisSyntaxError)
    }
  })

  test('invalid type - non-integer PX value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('PX'),
        Buffer.from('xyz'),
      ])
      assert.fail('Expected ExpectedInteger')
    } catch (err) {
      assert.ok(err instanceof ExpectedInteger)
    }
  })

  test('invalid type - non-integer EXAT value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('EXAT'),
        Buffer.from('not-a-number'),
      ])
      assert.fail('Expected ExpectedInteger')
    } catch (err) {
      assert.ok(err instanceof ExpectedInteger)
    }
  })

  test('invalid type - non-integer PXAT value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('PXAT'),
        Buffer.from('invalid'),
      ])
      assert.fail('Expected ExpectedInteger')
    } catch (err) {
      assert.ok(err instanceof ExpectedInteger)
    }
  })

  test('invalid expire time - negative PX value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('PX'),
        Buffer.from('-1000'),
      ])
      assert.fail('Expected InvalidExpireTime')
    } catch (err) {
      assert.ok(err instanceof InvalidExpireTime)
    }
  })

  test('invalid expire time - zero EXAT value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('EXAT'),
        Buffer.from('0'),
      ])
      assert.fail('Expected InvalidExpireTime')
    } catch (err) {
      assert.ok(err instanceof InvalidExpireTime)
    }
  })

  test('invalid expire time - negative PXAT value', async () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    try {
      await setCommand.run(Buffer.from('SET'), [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('PXAT'),
        Buffer.from('-500'),
      ])
      assert.fail('Expected InvalidExpireTime')
    } catch (err) {
      assert.ok(err instanceof InvalidExpireTime)
    }
  })

  test('getKeys method', () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    const keys = setCommand.getKeys(Buffer.from('SET'), [
      Buffer.from('mykey'),
      Buffer.from('myvalue'),
    ])

    assert.strictEqual(keys.length, 1)
    assert.strictEqual(keys[0].toString(), 'mykey')
  })

  test('getKeys with insufficient arguments', () => {
    const db = new DB()
    const setCommand = new SetCommand(db)

    assert.throws(
      () => setCommand.getKeys(Buffer.from('SET'), [Buffer.from('mykey')]),
      (err: Error) => err instanceof WrongNumberOfArguments,
    )
  })
})
