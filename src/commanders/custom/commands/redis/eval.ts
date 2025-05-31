import { LuaEngine } from 'wasmoon'
import crypto from 'crypto'
import { Command, CommandResult } from '../../../../types'
import {
  ExpectedInteger,
  UnknownScriptCommand,
  WrongNumberOfArguments,
  WrongNumberOfKeys,
} from '../../../../core/errors'

export class EvalCommand implements Command {
  constructor(
    private readonly lua: LuaEngine,
    private readonly commands: Record<string, Command>,
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

  async run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
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

        const { response } = await cmd.run(rawCmd, argsBuffer)

        let bufferRes

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
      },
    )

    // TODO add json support and msgpack
    const res = this.lua.doStringSync(`
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
}

export default function (
  lua: LuaEngine,
  commands: Record<string, Command>,
): Command {
  return new EvalCommand(lua, commands)
}
