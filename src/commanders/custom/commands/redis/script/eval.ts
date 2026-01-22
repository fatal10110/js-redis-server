import { UserFacedError, WrongNumberOfKeys } from '../../../../../core/errors'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'
import { replyValueToResponse } from './lua-reply'

type EvalArgs = [Buffer, number, Buffer[]]

export class EvalCommand extends SchemaCommand<EvalArgs> {
  metadata = defineCommand('eval', {
    arity: -3, // EVAL script numkeys [key ...] [arg ...]
    flags: {
      write: true,
      movablekeys: true,
      noscript: true,
    },
    categories: [CommandCategory.SCRIPT],
  })

  protected schema = t.tuple([
    t.key(),
    t.integer({ min: 0 }),
    t.variadic(t.key()),
  ])

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    const numKeys = parseInt(args[1]?.toString() ?? '', 10)
    if (!Number.isFinite(numKeys) || numKeys <= 0) {
      return []
    }

    return args.slice(2, 2 + numKeys)
  }

  protected execute([script, numKeys, rest]: EvalArgs, ctx: CommandContext) {
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
    ctx.transport.write(replyValueToResponse(reply, sha))
  }
}

function missingLuaRuntimeError(): Error {
  const err = new Error('Lua runtime is not initialized')
  err.name = 'ERR'
  return err
}
