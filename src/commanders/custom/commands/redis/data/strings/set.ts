import {
  InvalidExpireTime,
  RedisSyntaxError,
  WrongType,
} from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

interface SetOptions {
  expiration?: number
  nx?: boolean
  xx?: boolean
  keepTtl?: boolean
  get?: boolean
}

type SetTtlToken =
  | { type: 'EX'; value: number }
  | { type: 'PX'; value: number }
  | { type: 'EXAT'; value: number }
  | { type: 'PXAT'; value: number }

type SetSchemaOptions = Partial<{
  ttl: SetTtlToken
  condition: 'NX' | 'XX'
  keepTtl: 'KEEPTTL'
  get: 'GET'
}>

const metadata = defineCommand('set', {
  arity: -3, // SET key value [options...]
  flags: {
    write: true,
    denyoom: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.STRING],
})

export const SetCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, SetSchemaOptions]
> = {
  metadata,
  schema: t.tuple([
    t.key(),
    t.string(),
    t.options({
      ttl: t.xor([
        t.named('EX', t.integer({ min: 1 })),
        t.named('PX', t.integer({ min: 1 })),
        t.named('EXAT', t.integer({ min: 1 })),
        t.named('PXAT', t.integer({ min: 1 })),
      ]),
      condition: t.xor([t.flag('NX'), t.flag('XX')]),
      keepTtl: t.flag('KEEPTTL'),
      get: t.flag('GET'),
    }),
  ]),
  handler: ([key, value, schemaOptions], { db }) => {
    const options = parseOptions(schemaOptions ?? {})

    const existingData = db.get(key)
    let oldValue: Buffer | null = null

    if (options.get) {
      if (existingData instanceof StringDataType) {
        oldValue = existingData.data
      } else if (existingData !== null) {
        throw new WrongType()
      }
    }

    if (options.nx && existingData !== null) {
      if (options.get) {
        return oldValue
      }
      return null
    }

    if (options.xx && existingData === null) {
      if (options.get) {
        return null
      }
      return null
    }

    if (existingData !== null && !(existingData instanceof StringDataType)) {
      db.del(key)
    }

    let expiration: number | undefined

    if (options.keepTtl && existingData instanceof StringDataType) {
      expiration = undefined
    } else if (options.expiration !== undefined) {
      expiration = options.expiration
    }

    const valueBuffer = Buffer.from(value)
    db.set(key, new StringDataType(valueBuffer), expiration)

    if (options.get) {
      return oldValue
    }

    return 'OK'
  },
}

function parseOptions(schemaOptions: SetSchemaOptions): SetOptions {
  const options: SetOptions = {}

  if (schemaOptions.condition === 'NX') {
    options.nx = true
  } else if (schemaOptions.condition === 'XX') {
    options.xx = true
  }

  if (schemaOptions.keepTtl) {
    options.keepTtl = true
  }

  if (schemaOptions.get) {
    options.get = true
  }

  const ttl = schemaOptions.ttl
  if (ttl) {
    if (options.keepTtl) {
      throw new RedisSyntaxError()
    }
    if (ttl.type === 'EX') {
      if (ttl.value <= 0) {
        throw new InvalidExpireTime('set')
      }
      options.expiration = Date.now() + ttl.value * 1000
    } else if (ttl.type === 'PX') {
      if (ttl.value <= 0) {
        throw new InvalidExpireTime('set')
      }
      options.expiration = Date.now() + ttl.value
    } else if (ttl.type === 'EXAT') {
      if (ttl.value <= 0) {
        throw new InvalidExpireTime('set')
      }
      options.expiration = ttl.value * 1000
    } else if (ttl.type === 'PXAT') {
      if (ttl.value <= 0) {
        throw new InvalidExpireTime('set')
      }
      options.expiration = ttl.value
    } else {
      throw new RedisSyntaxError()
    }
  }

  if (options.nx && options.xx) {
    throw new RedisSyntaxError()
  }

  if (options.keepTtl && options.expiration !== undefined) {
    throw new RedisSyntaxError()
  }

  return options
}

export default function (db: DB) {
  return createSchemaCommand(SetCommandDefinition, { db })
}
