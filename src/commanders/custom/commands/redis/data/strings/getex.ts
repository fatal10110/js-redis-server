import {
  InvalidExpireTime,
  RedisSyntaxError,
  WrongType,
} from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

type GetexTtlToken =
  | {
      type: 'EX'
      value: number
    }
  | {
      type: 'PX'
      value: number
    }
  | {
      type: 'EXAT'
      value: number
    }
  | {
      type: 'PXAT'
      value: number
    }

type GetexSchemaOptions = Partial<{
  ttl: GetexTtlToken
  persist: 'PERSIST'
}>

export class GetexCommand extends SchemaCommand<[Buffer, GetexSchemaOptions]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('getex', {
    arity: -2, // GETEX key [options...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([
    t.key(),
    t.options({
      ttl: t.xor([
        t.named('EX', t.integer({ min: 1 })),
        t.named('PX', t.integer({ min: 1 })),
        t.named('EXAT', t.integer({ min: 1 })),
        t.named('PXAT', t.integer({ min: 1 })),
      ]),
      persist: t.flag('PERSIST'),
    }),
  ])

  protected execute(
    [key, schemaOptions]: [Buffer, GetexSchemaOptions],
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)

    if (existing === null) {
      transport.write(null)
      return
    }

    if (!(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    const options = schemaOptions ?? {}

    // Check for conflicting options
    if (options.ttl && options.persist) {
      throw new RedisSyntaxError()
    }

    // Update expiration if requested
    if (options.persist) {
      this.db.persist(key)
    } else if (options.ttl) {
      const ttl = options.ttl
      let expiration: number

      if (ttl.type === 'EX') {
        if (ttl.value <= 0) {
          throw new InvalidExpireTime('getex')
        }
        expiration = Date.now() + ttl.value * 1000
      } else if (ttl.type === 'PX') {
        if (ttl.value <= 0) {
          throw new InvalidExpireTime('getex')
        }
        expiration = Date.now() + ttl.value
      } else if (ttl.type === 'EXAT') {
        if (ttl.value <= 0) {
          throw new InvalidExpireTime('getex')
        }
        expiration = ttl.value * 1000
      } else if (ttl.type === 'PXAT') {
        if (ttl.value <= 0) {
          throw new InvalidExpireTime('getex')
        }
        expiration = ttl.value
      } else {
        throw new RedisSyntaxError()
      }

      this.db.set(key, existing, expiration)
    }

    transport.write(existing.data)
  }
}
