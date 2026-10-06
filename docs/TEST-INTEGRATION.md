# Integration Testing with Mock and Real Redis Clusters

This project includes a comprehensive integration testing system that can run tests against both our mock Redis cluster implementation and real Redis cluster instances. This dual-backend approach ensures our mock server behaves identically to the real Redis.

## Quick Start

### Run All Tests (Recommended)

```bash
npm run test:all
```

This will run the unit tests, followed by the complete integration test suite against both the mock and real Redis cluster backends.

### Run Tests Against Mock Cluster Only

```bash
npm run test:integration:mock
```

### Run Tests Against Real Redis Cluster Only

```bash
npm run test:integration:real
```

### Run a Single Suite / Backend

The integration suite is split by client and by transport so CI (and you) can run a slice:

```bash
npm run test:integration:mock:ioredis      # ioredis client, mock backend
npm run test:integration:mock:node-redis   # node-redis client, mock backend
npm run test:integration:raw:mock          # raw-tcp wire tests, mock backend
# ...and the matching :real:ioredis / :real:node-redis / raw:real variants
```

### Run the Suites Against the Socketless Client Mocks

```bash
npm run test:integration:socketless              # ioredis + node-redis suites
npm run test:integration:socketless:ioredis      # createIoredisMock only
npm run test:integration:socketless:node-redis   # createNodeRedisMock only
```

See [Socketless backend](#socketless-backend) below.

## Prerequisites

For testing against real Redis, you need:

- Docker and Docker Compose installed
- Ports available: **30000-30005** (cluster), **6399** (standalone), **6400** (password-protected standalone) — or any other free ports, see [Running a private stack](#running-a-private-stack)

## Test Infrastructure Management

### Start Real Redis Infrastructure

```bash
docker-compose -f docker-compose.test.yml up -d
```

This starts three services from the official `redis:8.0` image:

- **redis-cluster** — a 6-node cluster (3 masters + 3 replicas) on ports 30000-30005, all six `redis-server` processes in one container so MOVED/ASK redirects resolve from a Mac host.
- **redis-standalone** — a single non-cluster server on port 6399 (host) for SELECT / multi-database tests the cluster can't run.
- **redis-standalone-auth** — a `requirepass` server on port 6400 (password `testpass`) for AUTH/NOAUTH/WRONGPASS tests.

### Stop Real Redis Infrastructure

```bash
docker-compose -f docker-compose.test.yml down
```

Do **not** pass `-v` — the cluster's `nodes-*.conf` topology lives in the container and a volume wipe forces a slow re-form on next boot.

### Running a private stack

The default stack is one shared set of servers per machine, and `npm run test:integration:real*` starts by flushing it with `clean:redis`. Two runs against it at once — two worktrees, two checkouts, two agents — would wipe each other's keys mid-test (#497), so a run takes a lock on the stack first and a second run refuses to start (see [The run lock](#the-run-lock)). Give each concurrent run its own stack instead: every published port in `docker-compose.test.yml` can be moved, and the variables that move them are the ones the harness and `clean:redis` read, so exporting them once points the stack and the suite at the same servers.

```bash
export COMPOSE_PROJECT_NAME=redis-test-2           # separate containers and volumes
export REDIS_CLUSTER_PORT_RANGE=31000-31005        # six consecutive ports, first-last
export REDIS_STANDALONE_PORT=31006
export REDIS_STANDALONE_AUTH_PORT=31007

docker compose -f docker-compose.test.yml up -d --wait
npm run test:integration:real                       # flushes and tests this stack only
docker compose -f docker-compose.test.yml down
```

- `REDIS_CLUSTER_PORT_RANGE` must be six consecutive ports written `first-last`, ending at or below 55535 (each node's cluster bus is its client port + 10000, container-internal). `docker/redis-cluster-init.sh` binds the nodes to exactly those ports inside the container, so the host port map stays 1:1 and `MOVED` redirects still resolve from the host. The value is used verbatim — no surrounding whitespace. The init script and the harness accept exactly the same ranges (`tests/redis-endpoints.test.ts` runs the script against the harness to keep it that way), and both refuse a malformed one rather than falling back to the default; only an unset or empty variable means the default.
- When `REDIS_CLUSTER_PORTS` is also set it wins as the seed list, but every seed must lie inside the range: seeds pointing at a different cluster than the one the range describes are an error.
- `COMPOSE_PROJECT_NAME` keeps the second stack's containers and volumes apart from the first. Compose derives the default name from the checkout's directory, so separate worktrees already differ — what they shared was the ports. Set it explicitly when a second stack runs from the same directory, or `up` recreates the first one on the new ports.
- Unset, everything stays on the defaults (30000-30005, 6399, 6400), which is what CI uses.

### The run lock

`clean:redis` ([scripts/flush-redis.ts](../scripts/flush-redis.ts)) takes a run lock on the stack before it flushes anything (#542). When another run holds it, `clean:redis` flushes nothing, exits non-zero, and names the run that holds it:

```text
clean:redis refused to flush — another real-backend run holds this stack:
  cluster node 127.0.0.1:30000: held by js-redis-server-test-run:dev-box:22578:5afcee2d (from 172.17.0.1:51662, connected 41s ago)
  ...
What to do:
  - Another real-backend run holds this stack. Wait for it to finish, or start a private stack: see docs/TEST-INTEGRATION.md#running-a-private-stack
```

- The lock is a connection named `js-redis-server-test-run:<host>:<pid>:<nonce>` on every endpoint the run flushes: the configured cluster seeds, every node discovered from them, and the standalone servers. A run refuses if it sees another run's name on any of them. A run names all its connections before it lists any server's clients, so of two runs started at the same moment at most one goes on. Rarely, both refuse; start one again.
- The `test:integration:real*` scripts run the suite as a child of `flush-redis.ts -- <command>`. The lock stays held until the suite exits, and the script exits with the suite's status. A plain `npm run clean:redis` releases the lock as soon as the flush is verified.
- The lock is client connection state, not a key, so the suites' own `FLUSHALL`/`FLUSHDB` do not drop it. It has no TTL either: when a run dies, even by `kill -9`, the kernel closes its connections and the next run can start at once. The exception is a run whose `flush-redis.ts` process is killed while the suite it started keeps running: that suite is no longer covered. SIGTERM is passed on to the suite; Ctrl-C reaches it from the terminal.
- In CI each job has its own stack and takes the lock unopposed, so nothing changes there. The lock connections are idle and send nothing, so `MONITOR` tests do not see them, but they do appear in `CLIENT LIST`.

## How It Works

### Test Configuration System

The `tests-integration/test-config.ts` file provides a `TestRunner` class that abstracts the backend differences:

```typescript
import { testRunner } from '../test-config'

// The test runner automatically uses the correct backend
const cluster = await testRunner.setupIoredisCluster()
const backendName = testRunner.getBackendName() // "Mock Redis Server" or "Real Redis Server"
```

`TestRunner` exposes one setup method per (client × topology); each returns a connected client (or a port, for raw-tcp) and resolves `mock` to an in-process server, `real` to the docker-compose service:

| Method | Topology | Notes |
| --- | --- | --- |
| `setupIoredisCluster()` / `setupNodeRedisCluster()` | cluster | default 3 masters / 0 replicas; override via options |
| `setupIoredisStandalone()` / `setupNodeRedisStandalone()` | standalone (16 DBs) | for SELECT / multi-database tests |
| `setupIoredisStandaloneAuth()` / `setupNodeRedisStandaloneAuth()` | standalone + `requirepass` | client connects **without** a password; test drives AUTH |
| `setupRawCluster()` / `setupRawStandalone()` / `setupRawStandaloneAuth()` | returns port(s) only | raw-tcp tests open their own `RawRedisConnection` |

`mock` standalone servers spin up in-process via `Resp2Server`; `real` standalone connects to `REDIS_STANDALONE_PORT` / `REDIS_STANDALONE_AUTH_PORT` (set by docker-compose / CI), falling back to spawning a local `redis-server` child for dev.

### Environment Variable Control

Set the `TEST_BACKEND` environment variable to control which backend to use:

```bash
TEST_BACKEND=mock npm run test:integration:mock # Use mock cluster (default for this script)
TEST_BACKEND=real npm run test:integration:real # Use real Redis cluster
```

For the `real` backend, the standalone services are located via:

- `REDIS_STANDALONE_PORT` — host port of `redis-standalone` (6399 in docker-compose)
- `REDIS_STANDALONE_AUTH_PORT` — host port of `redis-standalone-auth` (6400 in docker-compose)

and the cluster via `REDIS_CLUSTER_PORTS` (comma-separated seed ports) or `REDIS_CLUSTER_PORT_RANGE` (`first-last`, the range a [private stack](#running-a-private-stack) was started on), defaulting to 30000-30005. docker-compose publishes its services on these same variables.

If unset, the harness spawns a local `redis-server` child as a dev fallback.

### Socketless backend

`TEST_BACKEND=socketless` runs the same `ioredis/**` and `node-redis/**` suites against the packaged socketless client mocks instead of a TCP server (#412), so a reply-shape divergence in those clients fails an integration test rather than surviving to review:

- `setupIoredisCluster()` / `setupIoredisStandalone()` return clients from `createIoredisMock()` — the real ioredis client over a virtual socket. Repeated cluster setups in one file are `duplicate()`s of one root, so they share its keyspace like the mock backend's shared cluster. Direct node connections (`connectToEndpoint()`, `RawRedisConnection.connect()`) resolve the mock's synthetic `host:port`s over the same virtual transport, and `getClusterPorts()` returns those synthetic ports.
- `setupNodeRedisCluster()` / `setupNodeRedisStandalone()` return `createNodeRedisMock()` facades (`NodeRedisMockCluster` / `NodeRedisMockClient`), cast to node-redis' types — a method the facade lacks fails the test that calls it. They are created with no `RESP` option, so like a default node-redis 6 client they run at RESP3, the protocol the node-redis suites run at against `mock`/`real`.
- Anything that needs a real port — `setupRawStandalone()`, `setupRawCluster()`, the requirepass setups — throws `SocketlessUnsupportedError`. A direct node connection made before any socketless cluster exists, or while two are open (their synthetic ports overlap), throws too instead of dialling TCP. `REDIS_COMPAT` is ignored.

The cases the socketless clients cannot pass yet are listed in [`tests-integration/socketless/known-gaps.ts`](../tests-integration/socketless/known-gaps.ts), not in the test files. The `socketless` scripts preload [`tests-integration/socketless/register.ts`](../tests-integration/socketless/register.ts), which marks each listed test `todo` (it still runs; its failure is reported but not fatal) or skips a file (or test) whose setup the backend cannot provide.

The list is strict:

- Every `todo` entry names its tests and the error they must fail with. Only `skip` entries may cover a whole file, so a test added to a listed file still has to pass or be listed.
- A test file fails if a listed title matches no test (or matches tests in more than one suite, when it must be given as its full `Suite > … > title` path), if a listed test passes, or if it fails with an error the entry does not expect — so a different failure cannot hide behind the todo.
- It also fails if a listed test's body never ran because a hook failed first. That is not the recorded cause, so the file needs a `skip` entry.
- Likewise, it fails if a listed test's body ran but never finished: node:test timed it out or cancelled it. A hang is a different failure too. `tests/socketless-register.test.ts` runs the preload against fixtures to pin each of these rules.
- Every entry's `file` must exist.

Fixing a divergence in `src/` therefore means deleting its entry. `skip` entries are not checked by a normal run; `SOCKETLESS_AUDIT_SKIPS=1` runs their files anyway and fails a file whose skipped tests now pass.

`tests-integration/node-redis/socketless-parity.test.ts` complements this from the other side. It runs on the TCP backends (`mock`, `real`) and compares the socketless clients' decoded replies with real node-redis's, for `createNodeRedisMock()` (standalone and `NodeRedisMockCluster`) and `createInMemoryRedis()`, at RESP2 and RESP3. On `real`, real Redis plus real node-redis is the oracle.

It also compares the defaults: a `createNodeRedisMock()` with no `RESP` option against a `createClient()` with none, which both negotiate RESP3, and the `resp=` that `CLIENT LIST` reports for each one's subscriber connection. The facade runs pub/sub on a dedicated session that takes the client's protocol. Its `(message, channel)` listener API hides the frame shape, so that `resp=` is how the suite observes it.

### Test Structure

Tests are structured to work with both backends seamlessly:

```typescript
describe(`String Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // Tests work identically on both backends
  test('INCR command', async () => {
    const result = await redisClient?.incr('counter')
    assert.strictEqual(result, 1)
  })
})
```

## Test Organization

```
tests-integration/
  ioredis/      # tests driving the ioredis client
  node-redis/   # tests driving the node-redis client
  raw-tcp/      # bytes-in/bytes-out wire tests over a bare RawRedisConnection
```

## Supported Client Libraries

The integration tests support both major Node.js Redis client libraries, against cluster and standalone:

### IORedis

- Cluster: `testRunner.setupIoredisCluster()`
- Standalone: `testRunner.setupIoredisStandalone()` / `setupIoredisStandaloneAuth()`

### node-redis

- Cluster: `testRunner.setupNodeRedisCluster()`
- Standalone: `testRunner.setupNodeRedisStandalone()` / `setupNodeRedisStandaloneAuth()`

## Docker Configuration

The `docker-compose.test.yml` file defines three services, all on the official `redis:8.0` image:

- **redis-cluster** — 6-node cluster on ports 30000-30005
- **redis-standalone** — single non-cluster server, host port 6399
- **redis-standalone-auth** — `requirepass` server (password `testpass`), host port 6400

### Cluster Configuration

- 3 master nodes + 3 replica nodes (6 total), formed with `--cluster-replicas 1`.
- Ports: 30000-30005 by default, or the six given by `REDIS_CLUSTER_PORT_RANGE` (bus ports, client port + 10000, stay container-internal).
- All six `redis-server` processes run in **one** container so they reach each other over 127.0.0.1 and the host port map is 1:1.
- Mac-compatible networking via `--cluster-announce-ip 127.0.0.1`, so MOVED/ASK redirects resolve from the host.
- Startup is driven by [`docker/redis-cluster-init.sh`](../docker/redis-cluster-init.sh), mounted read-only into the container. It wipes each node's data directory (below), waits for **all six** nodes to answer `PING` and report `cluster_enabled:1`, then retries `redis-cli --cluster create` (up to 3 times, resetting the nodes between attempts) until the readiness gate below passes.
- The readiness gate requires, **on every node**: `cluster_state:ok`, all 16384 slots assigned, `cluster_known_nodes:6`, and exactly 3 masters + 3 replicas with no `fail`/`fail?`/`handshake`/`noaddr` flags. Each condition catches something the others miss — in particular a dead *replica* leaves `cluster_state` at `ok`, because the surviving masters still cover every slot.
- Each `redis-server` logs to `/var/log/redis-cluster/<port>.log` inside the container; those logs plus every node's `CLUSTER INFO` and `CLUSTER NODES` are dumped to stdout if formation fails, so `docker compose logs redis-cluster` explains the failure.
- The healthcheck runs the same script in `check` mode, so the liveness probe applies exactly the gate above plus the ready marker. `docker compose up --wait` therefore cannot return while the cluster is still forming, and a node dying mid-run marks the container unhealthy.

- Each node runs in its own directory, `/data/<port>`, wiped on every boot. `/data` is a volume that survives `docker restart`, and a replica writes the RDB it receives during full sync there even with `--save ''`; with a shared directory all six nodes would reload that one file on the next boot and fail the create as "not empty".

**Timeout budget.** The init script is the authoritative budget: worst case 294s (35s node gate + 3 attempts x (45s create + 35s settle) + 2 x (5s reset + 2s backoff) + 5s diagnostics), after which it exits non-zero having dumped diagnostics. That bound holds even with a *hung* node, because every `redis-cli` call is capped at 5s and whenever all six nodes are queried they are queried in parallel, so a round costs one 5s cap at most. The healthcheck's `start_period` (330s) covers the whole script budget so a slow-but-healthy boot can never exhaust its retries, and the workflow's `--wait-timeout` (420s) is only an outer backstop. Keep that ordering if you change any of them — inverting it is how a failure ends up with no diagnostics at all.

Tunables (env vars on the `redis-cluster` service): `NODE_READY_TIMEOUT`, `CLUSTER_READY_TIMEOUT`, `CREATE_TIMEOUT`, `CREATE_ATTEMPTS`, `CLI_TIMEOUT`, `CLUSTER_NODE_TIMEOUT`, `DATA_DIR`, and `CLUSTER_PORT_RANGE` (set from `REDIS_CLUSTER_PORT_RANGE` by the compose file).

Either compose spelling works for the commands in this document: the `docker compose` v2 plugin and the standalone `docker-compose` v2 binary are the same implementation, and everything used here (`--wait`, `--wait-timeout`) needs only v2.17+.

## Benefits

1. **Confidence**: Tests pass on both mock and real Redis clusters.
2. **Compatibility**: Ensures mock cluster behavior matches real Redis.
3. **Speed**: Mock tests run faster for development.
4. **Isolation**: Each test run uses fresh Redis instances.
5. **CI/CD Ready**: Easy integration in continuous integration.

## CI/CD Integration

For continuous integration, you can run both test suites:

```yaml
# Example GitHub Actions
- name: Test against Mock Redis
  run: npm run test:integration:mock

- name: Test against Real Redis
  run: npm run test:integration:real
```

Or run the complete suite:

```yaml
- name: Run All Integration Tests
  run: npm run test:all
```

## Troubleshooting

### Port Conflicts

If you get port binding errors, ensure the cluster (30000-30005) and standalone (6399, 6400) ports are free:

```bash
# Check for any process using the cluster port range
lsof -i :30000-30005

# Standalone + auth ports
lsof -i :6399 -i :6400
```

### Docker Issues

Reset Docker state if needed:

```bash
docker-compose -f docker-compose.test.yml down
docker system prune -f
```

### Test Failures

When tests fail on real Redis but pass on mock:

1. Check Redis version compatibility.
2. Verify command implementation in the mock server.
3. Review timing-sensitive operations.

### Memory Issues

For large test suites, you might need to increase Node.js memory:

```bash
NODE_OPTIONS="--max-old-space-size=4096" npm run test:all
```
