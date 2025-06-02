import { ClusterNetwork } from './src/core/cluster/network'

async function run() {
  const cluster = new ClusterNetwork(console)

  process.on('SIGINT', async () => {
    await cluster.shutdown()
  })
  process.on('SIGABRT', async () => {
    await cluster.shutdown()
  })

  await cluster.init({ masters: 3, slaves: 0 })

  console.log(Array.from(cluster.getAll()).map(n => `${n.host}:${n.port}`))
}

run().catch(console.error)
