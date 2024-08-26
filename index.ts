import { createServer } from './src/network/server'

const server = createServer()
server.listen(7001, () => {
  console.log('opened server on', server.address())
})
