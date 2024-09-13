import { DataCommand } from '.'
import {
  ExpectedInteger,
  WrongNumberOfArguments,
  WrongNumberOfKeys,
} from '../../errors'
import { LuaFactory } from 'wasmoon'
import { Node } from '../../node'

export class EvalCommand implements DataCommand {
  private readonly factory: LuaFactory

  constructor() {
    this.factory = new LuaFactory()
  }

  getKeys(args: Buffer[]): Buffer[] {
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

  async run(node: Node, args: Buffer[]): Promise<Buffer> {
    const keys = []

    for (const key of this.getKeys(args)) {
      keys.push(`("${key.toString('hex')}"):fromhex()`)
    }

    const scriptArgs: string[] = []

    for (let i = 2 + keys.length; i < args.length; i++) {
      scriptArgs.push(`("${args[i].toString('hex')}"):fromhex()`)
    }

    const lua = await this.factory.createEngine({ injectObjects: true })

    try {
      // Set a JS function to be a global lua function
      // TODO args not only strings
      lua.global.set('redisCall', async (cmd: string, args: string[]) => {
        let { response: res } = await node.request(
          Buffer.from(cmd),
          args.map(arg => Buffer.from(arg, 'hex')),
        )

        if (res instanceof Promise) {
          res = await res
        }

        let bufferRes

        if (res instanceof Buffer) {
          bufferRes = res
        } else if (res instanceof Object && Object.hasOwn(res, 'toString')) {
          // TODO res can be any supported resp value, e.g list
          bufferRes = Buffer.from(res.toString())
        } else if (typeof res === 'string') {
          bufferRes = Buffer.from(res)
        } else {
          throw new Error(`Unsupported input of type ${typeof res}`)
        }

        return bufferRes.toString('hex')
      })

      // TODO add json support and msgpack
      const res = await lua.doString(`
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
          ${args[0].toString()}
        end

        local args = { ${scriptArgs.join(',')} }
        local keys = { ${keys.join(',')} }

        res = run(args, keys, redisInstance)
        -- TODO not always hexable
        return res:tohex()
      `)
      return Buffer.from(res, 'hex')
    } finally {
      // Close the lua environment, so it can be freed
      lua.global.close()
    }
  }
}

export default new EvalCommand()
