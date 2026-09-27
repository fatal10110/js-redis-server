# Contributing to js-redis-server

Thank you for your interest in contributing to js-redis-server!

## Development Setup

1. Fork and clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Build the project:
   ```bash
   npm run build
   ```
4. Run tests:
   ```bash
   npm test
   ```

## Making Changes

1. Create a new branch for your feature or fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and ensure:
   - All tests pass: `npm test`
   - Code is properly formatted: `npm run format`
   - Linting passes: `npm run lint -- .`

3. Commit your changes with a clear commit message

4. Push to your fork and submit a pull request

## Testing

We use Node.js built-in test runner with `node:test` and `node:assert`:

```typescript
import { test, describe } from 'node:test'
import assert from 'node:assert'

describe('MyFeature', () => {
  test('should do something', () => {
    assert.strictEqual(1 + 1, 2)
  })
})
```

### Running Tests

```bash
# Unit tests
npm test

# Integration tests with mock backend
npm run test:integration:mock

# Integration tests with real Redis (requires Redis cluster)
npm run test:integration:real

# All tests
npm run test:all
```

The real backend is a shared Redis cluster that is **not** flushed between test
files, so every integration test must namespace the keys it touches with
`randomKey()` (see `tests-integration/utils.ts`) — no fixed literal key names,
no assertions that depend on a key being absent at start, and no assertions on
total `DBSIZE`. The suite has to pass twice in a row without a flush in between.

`npm run test:integration:real` flushes first via `npm run clean:redis`, which
uses `scripts/flush-redis.ts` (no `redis-cli` required) and exits non-zero
unless every endpoint is verifiably empty and the cluster reports
`cluster_state:ok`. Its error output says what to do about each failure; start
the backends with `docker compose -f docker-compose.test.yml up -d --wait`
beforehand.

Because of that flush, two real-backend runs must never share one stack — a
second worktree or checkout would wipe the first one's keys mid-test (#497).
The harness, that script and `docker-compose.test.yml` all read the same env
vars, so a concurrent run starts its own stack on other ports:

```bash
export COMPOSE_PROJECT_NAME=redis-test-2
export REDIS_CLUSTER_PORT_RANGE=31000-31005
export REDIS_STANDALONE_PORT=31006
export REDIS_STANDALONE_AUTH_PORT=31007
docker compose -f docker-compose.test.yml up -d --wait
npm run test:integration:real
```

See [Running a private stack](docs/TEST-INTEGRATION.md#running-a-private-stack)
for the rules. The same variables also point the suite at a cluster you started
some other way:

```bash
REDIS_CLUSTER_PORTS=31100 \
REDIS_STANDALONE_PORT=7811 \
REDIS_STANDALONE_AUTH_PORT=7812 \
  npm run test:integration:real
```

`REDIS_CLUSTER_PORTS` is a comma-separated list of **seed** ports, defaulting
to `30000,30001,30002,30003,30004,30005`. One reachable node is enough: the
harness's cluster clients discover the rest from it, and `clean:redis` flushes
the whole topology it finds, failing if any node in it is unreachable. Ranges
like `30000-30005` are rejected there (use `REDIS_CLUSTER_PORT_RANGE` for a
range), and any malformed entry — in this, the range or either standalone
port — is an error instead of being skipped.

## Adding New Redis Commands

1. Create the command file in the appropriate directory:
   - Strings: `src/commanders/custom/commands/redis/data/strings/`
   - Hashes: `src/commanders/custom/commands/redis/data/hashes/`
   - Lists: `src/commanders/custom/commands/redis/data/lists/`
   - Sets: `src/commanders/custom/commands/redis/data/sets/`
   - Sorted Sets: `src/commanders/custom/commands/redis/data/zsets/`
   - Keys: `src/commanders/custom/commands/redis/data/keys/`

2. Implement the `Command` interface:
   ```typescript
   interface Command {
     readonly metadata: CommandMetadata
     getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[]
     run(rawCmd: Buffer, args: Buffer[], signal: AbortSignal, transport: Transport): CommandResult | void
   }
   ```

3. Register the command in `src/commanders/custom/commands/redis/index.ts`

4. Add tests for your command

## Code Style

- Use TypeScript
- Follow existing code patterns
- Use early returns to avoid nested conditions
- Prefer `for...of` with `Object.entries()` over `for...in`
- Minimize object allocations in hot paths

## Pull Request Guidelines

- Keep changes focused and atomic
- Include tests for new functionality
- Update documentation if needed
- Ensure CI passes before requesting review

## Changing the published API surface

The package publishes two entry points: the curated root (`src/index.ts`) and
the `/core` hand-wiring subpath (`src/internal.ts`). Both are public API.

Removing or renaming anything exported from either is a breaking change. Note it
under `Unreleased` in [CHANGELOG.md](CHANGELOG.md) in the same PR.

## Releasing both npm packages

`js-redis-server` and `js-valkey-server` share this repository, version, source,
and API. Neither name replaces the other. Keep the checked-in package name
`js-redis-server`; the release workflow selects the other name and its matching
CLI in a separate job, after installing dependencies from the shared lockfile.

Update the version in `package.json` and `package-lock.json` together, and
rename the `Unreleased` section in [CHANGELOG.md](CHANGELOG.md) to the new
version with its date. After CI passes, a `v<version>` tag runs both publish
jobs. The tag must match the package
version. The `NPM_TOKEN` secret needs permission to publish **both** names;
verify access to `js-valkey-server` before the first release.

Each job builds and tests its selected identity, including CommonJS, ESM, and
`/core`. npm cannot publish two packages atomically: if one job publishes and
the other fails, rerun only the failed job after resolving the failure. Do not
deprecate either package or change the GitHub repository/demo URLs.

## Questions?

Feel free to open an issue for any questions or discussions.
