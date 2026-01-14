import { NoScript } from '../../../../core/errors'
import { Command, ExecutionContext } from '../../../../types'
import { LuaEngine } from 'wasmoon'
import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import { createSchemaCommand, SchemaCommandRegistration, t } from '../../schema'
import { executeEvalScript, extractEvalKeys, splitEvalArguments } from './eval'

const metadata = defineCommand('evalsha', {
  arity: -3,
  flags: { write: true, noscript: true, movablekeys: true },
  firstKey: 0,
  lastKey: 0,
  keyStep: 0,
  categories: [CommandCategory.SCRIPT],
})

type EvalShaArgs = [string, number, Buffer[]]

export const EvalShaCommandDefinition: SchemaCommandRegistration<EvalShaArgs> =
  {
    metadata,
    schema: t.tuple([t.string(), t.integer(), t.variadic(t.key())]),
    getKeys: (_rawCmd, args) => extractEvalKeys('evalsha', args),
    handler: async ([sha, keyCount, rest], ctx) => {
      const script = ctx.db.getScript(sha)

      if (!script) {
        throw new NoScript()
      }

      const { keys, args } = splitEvalArguments(keyCount, rest)

      const luaEngine = ctx.luaEngine
      const commands = ctx.commands

      if (!luaEngine) {
        throw new Error('Lua engine is not available for EVALSHA')
      }

      if (!commands) {
        throw new Error('Command registry is not available for EVALSHA')
      }

      return executeEvalScript(script.toString(), keys, args, {
        luaEngine,
        commands,
        executionContext: ctx.executionContext,
        signal: ctx.signal,
      })
    },
  }

export default function (
  lua: LuaEngine,
  commands: Record<string, Command>,
  db?: DB,
  executionContext?: ExecutionContext,
): Command {
  if (!db) {
    throw new Error('DB is required for EVALSHA')
  }

  return createSchemaCommand(EvalShaCommandDefinition, {
    db,
    luaEngine: lua,
    commands,
    executionContext,
  })
}
