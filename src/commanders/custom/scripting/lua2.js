const fs = require('fs')
const { LuaFactory } = require('wasmoon')

// Initialize a new lua environment factory
// You can pass the wasm location as the first argument, useful if you are using wasmoon on a web environment and want to host the file by yourself
const factory = new LuaFactory()
// Create a standalone lua environment from the factory

async function run() {
  const lua = await factory.createEngine({ injectObjects: true })

  try {
    // Set a JS function to be a global lua function

    const buff = fs.readFileSync('./1px.png')
    //h = buff.toString('hex')
    //let s = '\\xd1\\x84\\xd0\\xb2\\xd1\\x84\\xd0\\xb2'
    //let s = ''

    // for (let i = 0; i < h.length; i += 2) {
    //   s += '\\' + 'x' + h[i] + h[i + 1]
    // }

    lua.global.set('ARGV', [buff.toString('base64'), Buffer.from('фвфв')])
    lua.global.set('test', arg => console.log(Array.isArray(arg)))
    const b = fs.readFileSync('./base64.lua').toString()

    const str = `${b}
    
    local decoded = base64.decode(ARGV[1])

    function string.tohex(str)
      return (str:gsub('.', function (c)
          return string.format('%02X', string.byte(c))
      end))
    end

    print(cjson.decode("[1]")[1])
    --decoded:tohex()

    return cjson.decode("[1]")
    `

    // Run a lua string
    const res = await lua.doString(str)
    //fs.writeFileSync('test.png', Buffer.from(buff.toString('hex'), 'hex'))

    // let z = ''

    // for (let i = 2; i < res.length; i += 4) {
    //   z += res[i] + res[i + 1]
    // }

    //fs.writeFileSync('test.png', Buffer.from(res, 'hex'))
    console.log(res.length)
    console.log(res)
    console.log(Array.isArray(res))
  } finally {
    // Close the lua environment, so it can be freed
    lua.global.close()
  }
}

run()
