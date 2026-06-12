import type { ExecutionPolicy } from './index'
import { NoAuthError } from '../redis-error'
import { RedisResult } from '../redis-result'

/**
 * Commands a client may issue before authenticating on a password-protected
 * server. Mirrors Redis' `no-auth` command flag: AUTH/HELLO let the client
 * authenticate, RESET clears connection state, QUIT closes the connection.
 * Everything else (including PING) is rejected with NOAUTH until the client
 * authenticates.
 */
const NO_AUTH_COMMANDS = new Set(['auth', 'hello', 'reset', 'quit'])

/**
 * Enforces `requirepass`: when the server is password-protected and the session
 * has not yet authenticated, every command outside {@link NO_AUTH_COMMANDS} is
 * short-circuited with `-NOAUTH Authentication required.`. When no password is
 * configured the policy is a no-op, so the default `nopass` user can run
 * anything.
 */
export function createAuthPolicy(): ExecutionPolicy {
  return {
    name: 'auth',
    beforeExecute(plan, ctx) {
      if (!ctx.server.requirepass) {
        return
      }

      if (ctx.session.isAuthenticated) {
        return
      }

      if (NO_AUTH_COMMANDS.has(plan.definition.name)) {
        return
      }

      const error = new NoAuthError()
      return RedisResult.error(error.message, error.code)
    },
  }
}
