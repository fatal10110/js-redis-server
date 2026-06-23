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
- Healthcheck waits for `cluster_state:ok` (every slot assigned) before the cluster is marked healthy.

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
