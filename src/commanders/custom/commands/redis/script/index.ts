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

export class ScriptCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand('script', {
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

  schema = t.tuple([t.string(), t.variadic(t.string())])

  handler(
    [subCommandName, rest]: [Buffer, Buffer[]],
    ctx: SchemaCommandContext,
  ) {
    const subCommands = createSubCommands(ctx)
    const subCommand = subCommands[subCommandName.toString().toLowerCase()]
    if (!subCommand) {
      throw new UnknowScriptSubCommand(subCommandName.toString())
    }
    subCommand.run(subCommandName, rest, ctx.signal, ctx.transport)
  }
}

function createSubCommands(ctx: SchemaCommandContext): Record<string, Command> {
  const baseCtx = {
    db: ctx.db,
    discoveryService: ctx.discoveryService,
    mySelfId: ctx.mySelfId,
  }
  return {
    load: createSchemaCommand(new ScriptLoadCommandDefinition(), baseCtx),
    exists: createSchemaCommand(new ScriptExistsCommandDefinition(), baseCtx),
    flush: createSchemaCommand(new ScriptFlushCommandDefinition(), baseCtx),
    kill: createSchemaCommand(new ScriptKillCommandDefinition(), baseCtx),
    debug: createSchemaCommand(new ScriptDebugCommandDefinition(), baseCtx),
    help: createSchemaCommand(new ScriptHelpCommandDefinition(), baseCtx),
  }
}

export default function (db: SchemaCommandContext['db']) {
  return createSchemaCommand(new ScriptCommandDefinition(), { db })
}
