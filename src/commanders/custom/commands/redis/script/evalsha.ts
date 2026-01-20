import { NoScript, WrongNumberOfKeys } from '../../../../../core/errors'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../schema'
import { replyValueToResponse } from './lua-reply'

type EvalShaArgs = [string, number, Buffer[]]

export class EvalShaCommandDefinition
  implements SchemaCommandRegistration<EvalShaArgs>
{
  metadata = defineCommand('evalsha', {
    arity: -3, // EVALSHA sha numkeys [key ...] [arg ...]
    flags: {
      write: true,
      movablekeys: true,
      noscript: true,
    },
    categories: [CommandCategory.SCRIPT],
  })

  schema = t.tuple([t.string(), t.integer({ min: 0 }), t.variadic(t.key())])

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    const numKeys = parseInt(args[1]?.toString() ?? '', 10)
    if (!Number.isFinite(numKeys) || numKeys <= 0) {
      return []
    }

    return args.slice(2, 2 + numKeys)
  }

  handler([sha, numKeys, rest]: EvalShaArgs, ctx: SchemaCommandContext) {
    if (numKeys > rest.length) {
      throw new WrongNumberOfKeys()
    }

    const script = ctx.db.getScript(sha)
    if (!script) {
      throw new NoScript()
    }

    const keys = rest.slice(0, numKeys)
    const argv = rest.slice(numKeys)
    const runtime = ctx.luaRuntime
    if (!runtime) {
      throw Error('Lua runtime is not initialized')
    }

    let reply
    try {
      reply =
        keys.length === 0 && argv.length === 0
          ? runtime.eval(script, ctx, sha)
          : runtime.evalWithArgs(script, keys, argv, ctx, sha)
    } catch (err) {
      if (err instanceof Error) {
        throw err
      }

      throw new Error(String(err))
    }
    ctx.transport.write(replyValueToResponse(reply, sha))
  }
}

export default function (db: DB) {
  return createSchemaCommand(new EvalShaCommandDefinition(), { db })
}
