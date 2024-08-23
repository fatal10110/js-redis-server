const Resp = require('respjs')
const net = require('net')
const RedisMock = require('ioredis-mock')

const redis = new RedisMock()

function prepareResponse(jsResponse) {
  if (jsResponse === null) {
    toWrite = Resp.encodeNull()
  } else if (Number.isInteger(jsResponse)) {
    toWrite = Resp.encodeInteger(jsResponse)
  } else if (Array.isArray(jsResponse)) {
    toWrite = Resp.encodeArray(jsResponse)
  } else if (Buffer.isBuffer(jsResponse)) {
    toWrite = Resp.encodeBufBulk(jsResponse)
  } else if (typeof jsResponse === 'object') {
    const keys = Object.keys(jsResponse);

    if (keys.length === 0) {
      toWrite = Resp.encodeNullArray()
    } else {
      const arr = []

      console.log(keys)
      console.log(JSON.stringify(jsResponse))

      for (const k of keys) {
        arr.push(Resp.encodeString(k))
        console.log(k)
        console.log(jsResponse[k])
        arr.push(prepareResponse(jsResponse[k]))
      }

      toWrite = Resp.encodeArray(arr)
    }
  } else {
    throw new Error(`Unknown response of type ${typeof jsResponse}`)
  }

  return toWrite;
}

const createResp = socket => {
  return new Resp({ bufBulk: false })
    .on('error', function (error) {
      socket.write(Resp.encodeError(error))
    })
    .on('data', function (data) {
      console.log(data)

      const [cmd, ...args] = data

      switch (cmd.toLowerCase()) {
        case 'command':
          socket.write(Resp.encodeString('mock command'))
          break
        case 'info':
          socket.write(Resp.encodeString('mock info'))
          break
        case 'ping':
          socket.write(Resp.encodeString('PONG'))
          break
        default:
          redis[`${cmd}Buffer`](...args).then(r => {
            try {
              socket.write(prepareResponse(r))
            } catch (err) {
              socket.write(Resp.encodeError(err))
            }
          })
      }
    })
}

const server = net
  .createServer({ keepAlive: true })
  .on('error', err => {
    // Handle errors here.
    throw err
  })
  .on('connection', socket => {
    const resp = createResp(socket)
    socket.pipe(resp)
  })

// Grab an arbitrary unused port.
server.listen(() => {
  console.log('opened server on', server.address())
})
