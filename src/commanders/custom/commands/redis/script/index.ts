import { UnknowScriptSubCommand } from '../../../../../core/errors'
import { Command } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandContext,
  SchemaCommandRegistration,
  t,
} from '../../../schema'
import { ScriptLoadCommandDefinition } from './load'
import { ScriptExistsCommandDefinition } from './exists'
import { ScriptFlushCommandDefinition } from './flush'
import { ScriptKillCommandDefinition } from './kill'
import { ScriptDebugCommandDefinition } from './debug'
import { ScriptHelpCommandDefinition } from './help'

const metadata = defineCommand('script', {
  arity: -2, // SCRIPT subcommand [args...]
  flags: {
    admin: true,
    noscript: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SCRIPT],
})

export const ScriptCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.variadic(t.key())]),
  handler: async ([subCommandName, rest], ctx) => {
    const subCommands = createSubCommands(ctx)
    const subCommand = subCommands[subCommandName.toString().toLowerCase()]

    if (!subCommand) {
      throw new UnknowScriptSubCommand(subCommandName.toString())
    }

    return subCommand.run(subCommandName, rest, ctx.signal)
  },
}

function createSubCommands(ctx: SchemaCommandContext): Record<string, Command> {
  const baseCtx = {
    db: ctx.db,
    luaEngine: ctx.luaEngine,
    discoveryService: ctx.discoveryService,
    mySelfId: ctx.mySelfId,
  }

  return {
    load: createSchemaCommand(ScriptLoadCommandDefinition, baseCtx),
    exists: createSchemaCommand(ScriptExistsCommandDefinition, baseCtx),
    flush: createSchemaCommand(ScriptFlushCommandDefinition, baseCtx),
    kill: createSchemaCommand(ScriptKillCommandDefinition, baseCtx),
    debug: createSchemaCommand(ScriptDebugCommandDefinition, baseCtx),
    help: createSchemaCommand(ScriptHelpCommandDefinition, baseCtx),
  }
}

export default function (db: SchemaCommandContext['db']) {
  return createSchemaCommand(ScriptCommandDefinition, { db })
}
