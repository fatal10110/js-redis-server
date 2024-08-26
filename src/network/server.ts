import net, { Socket } from 'net'
import Resp from 'respjs'
import { RequestHandler } from '../core/request'
import { Node } from '../core/node'

function prepareResponse(jsResponse: unknown): Buffer {
  if (jsResponse === null) {
    return Resp.encodeNull()
  } else if (Number.isInteger(jsResponse)) {
    return Resp.encodeInteger(jsResponse as number)
  } else if (Array.isArray(jsResponse)) {
    return Resp.encodeArray(jsResponse)
  } else if (Buffer.isBuffer(jsResponse)) {
    return Resp.encodeBufBulk(jsResponse)
  } else if (typeof jsResponse === 'string') {
    return Resp.encodeString(jsResponse)
  } else if (typeof jsResponse === 'object') {
    const keys = Object.keys(jsResponse)

    if (keys.length === 0) {
      return Resp.encodeNullArray()
    } else {
      const arr = []

      for (const k of keys) {
        arr.push(Resp.encodeString(k))
        arr.push(prepareResponse((jsResponse as Record<string, unknown>)[k]))
      }

      return Resp.encodeArray(arr)
    }
  } else {
    throw new Error(`Unknown response of type ${typeof jsResponse}`)
  }
}

function createResp(socket: Socket, handler: RequestHandler) {
  return new Resp({ bufBulk: true })
    .on('error', function (error: unknown) {
      socket.write(Resp.encodeError(error as Error))
    })
    .on('data', function (data: Buffer[]) {
      const [cmd, ...args] = data
      let responseData: Buffer

      try {
        responseData = prepareResponse(handler.handleRequest(cmd, args))
      } catch (err) {
        responseData = Resp.encodeError(err as Error)
      }

      socket.write(responseData)
    })
}

export function createServer() {
  const node = new Node()

  return net
    .createServer({ keepAlive: true })
    .on('error', err => {
      // Handle errors here.
      throw err
    })
    .on('connection', socket => {
      const resp = createResp(socket, node.requestHandler)
      socket.pipe(resp)
    })
}
