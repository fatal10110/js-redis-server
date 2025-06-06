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

## Prerequisites

For testing against a real Redis cluster, you need:

- Docker and Docker Compose installed
- Ports 30000-30005 available

## Test Infrastructure Management

### Start Real Redis Infrastructure

```bash
docker-compose -f docker-compose.test.yml up -d
```

This starts a Redis cluster with 3 masters and 3 slaves on ports 30000-30005, using the `grokzen/redis-cluster` image.

### Stop Real Redis Infrastructure

```bash
docker-compose -f docker-compose.test.yml down
```

## How It Works

### Test Configuration System

The `tests-integration/test-config.ts` file provides a `TestRunner` class that abstracts the backend differences:

```typescript
import { testRunner } from '../test-config'

// The test runner automatically uses the correct backend
const cluster = await testRunner.setupIoredisCluster()
const backendName = testRunner.getBackendName() // "Mock Redis Server" or "Real Redis Server"
```

### Environment Variable Control

Set the `TEST_BACKEND` environment variable to control which backend to use:

```bash
TEST_BACKEND=mock npm run test:integration:mock # Use mock cluster (default for this script)
TEST_BACKEND=real npm run test:integration:real # Use real Redis cluster
```

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

## Supported Client Libraries

The integration tests support cluster clients from both major Node.js Redis client libraries:

### IORedis

- Cluster support: `testRunner.setupIoredisCluster()`

### node-redis

- Cluster support: `testRunner.setupNodeRedisCluster()`

## Docker Configuration

The `docker-compose.test.yml` file defines:

- **redis-cluster**: A pre-built Redis cluster using the `grokzen/redis-cluster` image.

### Cluster Configuration

- 3 master nodes + 3 slave nodes (6 total)
- Ports: 30000-30005
- Uses `grokzen/redis-cluster` Docker image for simplified setup.
- Automatically configures cluster topology.
- Mac-compatible networking configuration via `IP: 0.0.0.0`.

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

If you get port binding errors, ensure ports 30000-30005 are free:

```bash
# Check for any process using the port range
lsof -i :30000-30005

# Or check individual ports
for port in 30000 30001 30002 30003 30004 30005; do lsof -i :$port; done
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
