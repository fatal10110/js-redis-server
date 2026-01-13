import { LuaEngine } from 'wasmoon'
import crypto from 'crypto'
import {
  Command,
  CommandResult,
  ExecutionContext,
  LockContext,
} from '../../../../types'
import {
  ExpectedInteger,
  UnknownScriptCommand,
  WrongNumberOfArguments,
  WrongNumberOfKeys,
} from '../../../../core/errors'
import { LuaTransport } from '../../lua-transport'
import { DB } from '../../db'
import type { CommandMetadata } from '../../commands/metadata'
import { CommandCategory } from '../../commands/metadata'

export class EvalCommand implements Command {
  readonly metadata: CommandMetadata = {
    name: 'eval',
    arity: -3,
    flags: { write: true, noscript: true, movablekeys: true },
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: [CommandCategory.SCRIPT],
  }

  constructor(
    private readonly lua: LuaEngine,
    private readonly commands: Record<string, Command>,
    private readonly db: DB,
    private readonly executionContext?: ExecutionContext,
  ) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('eval')
    }

    const keysNum = Number(args[1].toString())

    if (isNaN(keysNum)) {
      throw new ExpectedInteger()
    }

    if (args.length - 2 < keysNum) {
      throw new WrongNumberOfKeys()
    }

    const keys: Buffer[] = []

    for (let i = 0; i < keysNum; i++) {
      keys.push(args[2 + i])
    }

    return keys
  }

  async run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult> {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('eval')
    }

    const script = args[0].toString()
    const sha = crypto.createHash('sha1').update(script).digest('hex')

    const keys = []

    for (const key of this.getKeys(rawCmd, args)) {
      keys.push(`("${key.toString('hex')}"):fromhex()`)
    }

    const scriptArgs: string[] = []

    for (let i = 2 + keys.length; i < args.length; i++) {
      scriptArgs.push(`("${args[i].toString('hex')}"):fromhex()`)
    }

    // If execution context is available, use it for atomic execution
    if (this.executionContext) {
      return this.runWithContext(script, keys, scriptArgs, sha, signal)
    }

    // Fallback to old implementation (for backward compatibility)
    return this.runLegacy(script, keys, scriptArgs, sha, signal)
  }

  private async runWithContext(
    script: string,
    keys: string[],
    scriptArgs: string[],
    sha: string,
    signal: AbortSignal,
  ): Promise<CommandResult> {
    // CRITICAL: Acquire lock ONCE for entire script
    const release = await this.db.lock.acquire()

    try {
      // Create special transport for Lua
      const luaTransport = new LuaTransport()

      // Set up redis.call to execute without re-acquiring lock
      this.lua.global.set(
        'redisCall',
        async (cmdName: string, luaArgs: string[]) => {
          const cmd = this.commands[cmdName.toLowerCase()]

          if (!cmd) {
            throw new UnknownScriptCommand(sha)
          }

          const argsBuffer = luaArgs.map(arg => Buffer.from(arg, 'hex'))

          // Reset transport for this command
          luaTransport.reset()

          // CRITICAL: Pass lockContext to indicate lock is already held
          const lockContext: LockContext = { lockHeld: true }

          await this.executionContext!.execute(
            luaTransport,
            Buffer.from(cmdName),
            argsBuffer,
            signal,
            lockContext,
          )

          // Get response from transport
          const response = luaTransport.getResponse()

          // Convert Redis response to Lua format
          return this.convertResponseToHex(response)
        },
      )

      // Execute script
      const luaScript = `
        function string.fromhex(str)
            return (str:gsub('..', function (cc)
                return string.char(tonumber(cc, 16))
            end))
        end

        function string.tohex(str)
            return (str:gsub('.', function (c)
                return string.format('%02X', string.byte(c))
            end))
        end

        redisInstance = {}

        redisInstance.call = function (cmd, ...)
          local args = {...}

          for i, v in ipairs(args) do
            args[i] = v:tohex()
          end

          local res = redisCall(cmd, args):await()

          -- TODO res not always hex string
          return res:fromhex()
        end

        function run(ARGV, KEYS, redis)
          ${script}
        end

        -- TODO hex
        local args = { ${scriptArgs.join(',')} }
        -- TODO hex
        local keys = { ${keys.join(',')} }

        res = run(args, keys, redisInstance)
        -- TODO not always hexable
        return res:tohex()
      `

      const result = await this.lua.doString(luaScript)

      return { response: Buffer.from(result, 'hex') }
    } finally {
      // RELEASE lock ONCE after script completes
      release()
    }
  }

  private async runLegacy(
    script: string,
    keys: string[],
    scriptArgs: string[],
    sha: string,
    signal: AbortSignal,
  ): Promise<CommandResult> {
    // Set a JS function to be a global lua function
    // TODO args not only strings
    // TODO there is race condition here
    this.lua.global.set(
      'redisCall',
      async (cmdName: string, args: string[]) => {
        const rawCmd = Buffer.from(cmdName)
        const argsBuffer = args.map(arg => Buffer.from(arg, 'hex'))
        const cmd = this.commands[cmdName]

        if (!cmd) {
          throw new UnknownScriptCommand(sha)
        }

        const { response } = await cmd.run(rawCmd, argsBuffer, signal)

        return this.convertResponseToHex(response)
      },
    )

    // TODO add json support and msgpack
    const res = await this.lua.doString(`
      function string.fromhex(str)
          return (str:gsub('..', function (cc)
              return string.char(tonumber(cc, 16))
          end))
      end

      function string.tohex(str)
          return (str:gsub('.', function (c)
              return string.format('%02X', string.byte(c))
          end))
      end

      redisInstance = {}

      redisInstance.call = function (cmd, ...)
        local args = {...}

        for i, v in ipairs(args) do
          args[i] = v:tohex()
        end

        local res = redisCall(cmd, args):await()

        -- TODO res not always hex string
        return res:fromhex()
      end

      function run(ARGV, KEYS, redis)
        ${script}
      end

      -- TODO hex
      local args = { ${scriptArgs.join(',')} }
      -- TODO hex
      local keys = { ${keys.join(',')} }

      res = run(args, keys, redisInstance)
      -- TODO not always hexable
      return res:tohex()
    `)

    return { response: Buffer.from(res, 'hex') }
  }

  private convertResponseToHex(response: unknown): string {
    let bufferRes: Buffer

    if (response instanceof Buffer) {
      bufferRes = response
    } else if (
      response instanceof Object &&
      Object.hasOwn(response, 'toString')
    ) {
      // TODO res can be any supported resp value, e.g list
      bufferRes = Buffer.from(response.toString())
    } else if (typeof response === 'string') {
      bufferRes = Buffer.from(response)
    } else {
      throw new Error(`Unsupported input of type ${typeof response}`)
    }

    return bufferRes.toString('hex')
  }
}

export default function (
  lua: LuaEngine,
  commands: Record<string, Command>,
  db?: DB,
  executionContext?: ExecutionContext,
): Command {
  return new EvalCommand(lua, commands, db!, executionContext)
}
