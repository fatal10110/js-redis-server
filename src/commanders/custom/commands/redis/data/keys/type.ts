import { StringDataType } from '../../../../data-structures/string'
import { HashDataType } from '../../../../data-structures/hash'
import { ListDataType } from '../../../../data-structures/list'
import { SetDataType } from '../../../../data-structures/set'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('type', {
  arity: 2, // TYPE key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.GENERIC],
})

export const TypeCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: async ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: 'none' }
    }

    if (existing instanceof StringDataType) {
      return { response: 'string' }
    }
    if (existing instanceof HashDataType) {
      return { response: 'hash' }
    }
    if (existing instanceof ListDataType) {
      return { response: 'list' }
    }
    if (existing instanceof SetDataType) {
      return { response: 'set' }
    }
    if (existing instanceof SortedSetDataType) {
      return { response: 'zset' }
    }

    return { response: 'unknown' }
  },
}

export default function (db: DB) {
  return createSchemaCommand(TypeCommandDefinition, { db })
}
