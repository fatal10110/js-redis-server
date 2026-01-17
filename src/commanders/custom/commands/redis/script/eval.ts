import { UserFacedError, WrongNumberOfKeys } from '../../../../../core/errors'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'
import { replyValueToResponse } from './lua-reply'

const metadata = defineCommand('eval', {
  arity: -3, // EVAL script numkeys [key ...] [arg ...]
  flags: {
    write: true,
    movablekeys: true,
    noscript: true,
  },
  categories: [CommandCategory.SCRIPT],
})

type EvalArgs = [Buffer, number, Buffer[]]

export const EvalCommandDefinition: SchemaCommandRegistration<EvalArgs> = {
  metadata,
  schema: t.tuple([t.key(), t.integer({ min: 0 }), t.variadic(t.key())]),
  getKeys: (_rawCmd, args) => {
    const numKeys = parseInt(args[1]?.toString() ?? '', 10)
    if (!Number.isFinite(numKeys) || numKeys <= 0) {
      return []
    }

    return args.slice(2, 2 + numKeys)
  },
  handler: ([script, numKeys, rest], ctx) => {
    if (numKeys > rest.length) {
      throw new WrongNumberOfKeys()
    }

    const keys = rest.slice(0, numKeys)
    const argv = rest.slice(numKeys)
    const sha = ctx.db.addScript(script)
    const runtime = ctx.luaRuntime
    if (!runtime) {
      throw missingLuaRuntimeError()
    }

    let reply
    try {
      reply =
        keys.length === 0 && argv.length === 0
          ? runtime.eval(script, ctx, sha)
          : runtime.evalWithArgs(script, keys, argv, ctx, sha)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      throw new UserFacedError(message)
    }
    ctx.transport.write(
      replyValueToResponse(reply, sha, message => new UserFacedError(message)),
    )
  },
}

export default function (db: DB) {
  return createSchemaCommand(EvalCommandDefinition, { db })
}

function missingLuaRuntimeError(): Error {
  const err = new Error('Lua runtime is not initialized')
  err.name = 'ERR'
  return err
}
