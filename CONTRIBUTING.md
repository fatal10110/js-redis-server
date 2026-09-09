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

## Releasing both npm packages

`js-redis-server` and `js-valkey-server` share this repository, version, source,
and API. Neither name replaces the other. Keep the checked-in package name
`js-redis-server`; the release workflow selects the other name and its matching
CLI in a separate job, after installing dependencies from the shared lockfile.

Update the version in `package.json` and `package-lock.json` together. After CI
passes, a `v<version>` tag runs both publish jobs. The tag must match the package
version. The `NPM_TOKEN` secret needs permission to publish **both** names;
verify access to `js-valkey-server` before the first release.

Each job builds and tests its selected identity, including CommonJS, ESM, and
`/core`. npm cannot publish two packages atomically: if one job publishes and
the other fails, rerun only the failed job after resolving the failure. Do not
deprecate either package or change the GitHub repository/demo URLs.

## Questions?

Feel free to open an issue for any questions or discussions.
