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
- Ports available: **30000-30005** (cluster), **6399** (standalone), **6400** (password-protected standalone)

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

If unset, the harness spawns a local `redis-server` child as a dev fallback.

### Socketless backend

`TEST_BACKEND=socketless` runs the same `ioredis/**` and `node-redis/**` suites against the packaged socketless client mocks instead of a TCP server (#412), so a reply-shape divergence in those clients fails an integration test rather than surviving to review:

- `setupIoredisCluster()` / `setupIoredisStandalone()` return clients from `createIoredisMock()` — the real ioredis client over a virtual socket. Repeated cluster setups in one file are `duplicate()`s of one root, so they share its keyspace like the mock backend's shared cluster. Direct node connections (`connectToEndpoint()`, `RawRedisConnection.connect()`) resolve the mock's synthetic `host:port`s over the same virtual transport, and `getClusterPorts()` returns those synthetic ports.
- `setupNodeRedisCluster()` / `setupNodeRedisStandalone()` return `createNodeRedisMock()` facades (`NodeRedisMockCluster` / `NodeRedisMockClient`), cast to node-redis' types — a method the facade lacks fails the test that calls it. The harness sends each facade `HELLO 3` first, because node-redis 6 defaults to RESP3 and that is the protocol the node-redis suites run at against `mock`/`real`.
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

One known divergence is in the facade's default, not in any reply shape: `createNodeRedisMock()` starts on RESP2, while a default node-redis 6 client negotiates RESP3. Out of the box, the facade returns ZSCORE as `'2.5'` and HGETALL as `['f', 'v']`, where node-redis returns `2.5` and `{ f: 'v' }`. The parity suite pins this with a test that asserts today's divergence, so it fails once the facade is fixed (`FACADE_DEFAULT_PROTOCOL` in `known-gaps.ts`). The fix, defaulting the facade to RESP3, is a `src/` follow-up.

A second one concerns pub/sub. The facade opens a dedicated session for pub/sub on first subscribe, and that session stays on RESP2 even after `HELLO 3` on the client. Its `(message, channel)` listener API hides the frame shape, so the parity suite claims delivery parity only (`FACADE_PUBSUB_PROTOCOL`). Opening that session at the client's protocol is also a `src/` follow-up.

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
- Ports: 30000-30005 (bus ports 40000-4000x stay container-internal).
- All six `redis-server` processes run in **one** container so they reach each other over 127.0.0.1 and the host port map is 1:1.
- Mac-compatible networking via `--cluster-announce-ip 127.0.0.1`, so MOVED/ASK redirects resolve from the host.
- Startup is driven by [`docker/redis-cluster-init.sh`](../docker/redis-cluster-init.sh), mounted read-only into the container. It wipes each node's data directory (below), waits for **all six** nodes to answer `PING` and report `cluster_enabled:1`, then retries `redis-cli --cluster create` (up to 3 times, resetting the nodes between attempts) until the readiness gate below passes.
- The readiness gate requires, **on every node**: `cluster_state:ok`, all 16384 slots assigned, `cluster_known_nodes:6`, and exactly 3 masters + 3 replicas with no `fail`/`fail?`/`handshake`/`noaddr` flags. Each condition catches something the others miss — in particular a dead *replica* leaves `cluster_state` at `ok`, because the surviving masters still cover every slot.
- Each `redis-server` logs to `/var/log/redis-cluster/<port>.log` inside the container; those logs plus every node's `CLUSTER INFO` and `CLUSTER NODES` are dumped to stdout if formation fails, so `docker compose logs redis-cluster` explains the failure.
- The healthcheck runs the same script in `check` mode, so the liveness probe applies exactly the gate above plus the ready marker. `docker compose up --wait` therefore cannot return while the cluster is still forming, and a node dying mid-run marks the container unhealthy.

- Each node runs in its own directory, `/data/<port>`, wiped on every boot. `/data` is a volume that survives `docker restart`, and a replica writes the RDB it receives during full sync there even with `--save ''`; with a shared directory all six nodes would reload that one file on the next boot and fail the create as "not empty".

**Timeout budget.** The init script is the authoritative budget: worst case 294s (35s node gate + 3 attempts x (45s create + 35s settle) + 2 x (5s reset + 2s backoff) + 5s diagnostics), after which it exits non-zero having dumped diagnostics. That bound holds even with a *hung* node, because every `redis-cli` call is capped at 5s and whenever all six nodes are queried they are queried in parallel, so a round costs one 5s cap at most. The healthcheck's `start_period` (330s) covers the whole script budget so a slow-but-healthy boot can never exhaust its retries, and the workflow's `--wait-timeout` (420s) is only an outer backstop. Keep that ordering if you change any of them — inverting it is how a failure ends up with no diagnostics at all.

Tunables (env vars on the `redis-cluster` service): `NODE_READY_TIMEOUT`, `CLUSTER_READY_TIMEOUT`, `CREATE_TIMEOUT`, `CREATE_ATTEMPTS`, `CLI_TIMEOUT`, `CLUSTER_NODE_TIMEOUT`, `DATA_DIR`.

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
