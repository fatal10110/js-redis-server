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

  console.log(
    Array.from(cluster.getAll()).map(n => ({
      port: n.getAddress().port,
      slots: n.slotRange,
    })),
  )
}

run().catch(console.error)
