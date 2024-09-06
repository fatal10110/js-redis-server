import { ClusterNetwork } from './src/core/cluster/network'

async function run() {
  const cluster = new ClusterNetwork(console)

  process.on('SIGINT', async () => {
    await cluster.shutdown()
  })
  process.on('SIGABRT', async () => {
    await cluster.shutdown()
  })

  await cluster.init({ masters: 3, slaves: 2 })
}

run().catch(console.error)
