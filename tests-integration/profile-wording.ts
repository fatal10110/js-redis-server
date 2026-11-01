import { createHash } from 'node:crypto'
import { errorWithMessage, activeProfile } from './utils'

/**
 * Error wording that differs by compatibility profile, for the suites that run
 * once per `REDIS_COMPAT` profile (`npm run test:integration:profiles:mock`)
 * and against real servers of that version. Every string below is a captured
 * real reply (redis 6.2.14, 7.0.15 - 8.0.0, valkey 8.0.0 and 9.0.0).
 */

/** Redis 6.2 words most script and subcommand errors its own way. */
export const legacy = activeProfile === 'redis-6.2'
export const isValkey = activeProfile.startsWith('valkey-')

export function scriptSha(script: string): string {
  return createHash('sha1').update(script).digest('hex')
}

/** The reply to EVALSHA of a script that is not cached (Valkey drops the hint). */
export const NOSCRIPT = isValkey
  ? 'NOSCRIPT No matching script.'
  : 'NOSCRIPT No matching script. Please use EVAL.'

/** `redis.call` / `redis.pcall` library failures, without the `ERR ` code. */
export const SCRIPT_LIB = {
  unknownCommand: legacy
    ? 'Unknown Redis command called from Lua script'
    : isValkey
      ? 'Unknown command called from script'
      : 'Unknown Redis command called from script',
  noArguments: legacy
    ? 'Please specify at least one argument for redis.call()'
    : isValkey
      ? 'Please specify at least one argument for this call'
      : 'Please specify at least one argument for this redis lib call',
  badArgument: legacy
    ? 'Lua redis() command arguments must be strings or integers'
    : isValkey
      ? 'Command arguments must be strings or integers'
      : 'Lua redis lib command arguments must be strings or integers',
  notAllowed: legacy
    ? 'This Redis command is not allowed from scripts'
    : activeProfile === 'valkey-9.0'
      ? 'This Valkey command is not allowed from script'
      : 'This Redis command is not allowed from script',
}

/**
 * The reply of a script aborted by a failed `redis.call` (the failure is a
 * Lua error raised by the library: unknown command, no argument, ...).
 */
export function libCallAbort(
  script: string,
  message: string,
  line = 1,
): string {
  const sha = scriptSha(script)
  return legacy
    ? `ERR Error running script (call to f_${sha}): @user_script:${line}: @user_script: ${line}: ${message}`
    : `ERR ${message} script: ${sha}, on @user_script:${line}.`
}

/**
 * The reply of a script aborted by an error reply of a command it called
 * through `redis.call` (`WRONGTYPE ...`). `message` carries its own code.
 */
export function commandCallAbort(
  script: string,
  message: string,
  line = 1,
): string {
  const sha = scriptSha(script)
  return legacy
    ? `ERR Error running script (call to f_${sha}): @user_script:${line}: ${message}`
    : `${message} script: ${sha}, on @user_script:${line}.`
}

/**
 * The reply of a script aborted by a Lua runtime error (nil call, readonly
 * table write, `error('x')`). `message` is Lua's own text, without the
 * `user_script:<line>:` chunk name.
 */
export function luaRuntimeAbort(
  script: string,
  message: string,
  line = 1,
): string {
  const sha = scriptSha(script)
  return legacy
    ? `ERR Error running script (call to f_${sha}): @user_script:${line}: user_script:${line}: ${message}`
    : `ERR user_script:${line}: ${message} script: ${sha}, on @user_script:${line}.`
}

/** What `redis.pcall` returns for a library failure (a one-line script). */
export function libPcallRejection(
  message: string,
): (error: unknown) => boolean {
  return errorWithMessage(
    legacy ? `@user_script: 1: ${message}` : `ERR ${message}`,
  )
}

/** `redis.error_reply('foo')`: 6.2 adds no `ERR ` code. */
export const ERROR_REPLY_FOO = legacy ? 'foo' : 'ERR foo'

/** `<CONTAINER> <unknown>`: 6.2 folds it into the "or wrong number" form. */
export function unknownSubcommand(container: string, sub: string): string {
  return legacy
    ? `ERR Unknown subcommand or wrong number of arguments for '${sub}'. Try ${container.toUpperCase()} HELP.`
    : `ERR unknown subcommand '${sub}'. Try ${container.toUpperCase()} HELP.`
}

/** A known subcommand called with the wrong arity (`CONFIG GET`, `SCRIPT LOAD`). */
export function subcommandArity(container: string, sub: string): string {
  return legacy
    ? `ERR Unknown subcommand or wrong number of arguments for '${sub.toUpperCase()}'. Try ${container.toUpperCase()} HELP.`
    : `ERR wrong number of arguments for '${container.toLowerCase()}|${sub.toLowerCase()}' command`
}

/**
 * GEO unit errors: redis 6.2 lists the units in lower case, 7.0+ and valkey
 * in upper case (`geo.unit-case-folding`).
 */
export const GEO_UNIT_ERROR = legacy
  ? 'ERR unsupported unit provided. please use m, km, ft, mi'
  : 'ERR unsupported unit provided. please use M, KM, FT, MI'

/**
 * STORE / STOREDIST / GEOSEARCHSTORE combined with WITHDIST, WITHHASH or
 * WITHCOORD: redis 6.2 answers the GEORADIUS wording (`...COORDS options`)
 * for every command, 7.0+ and valkey name the command
 * (`geo.store-with-options-wording`).
 */
export function geoStoreWithError(
  command: 'GEORADIUS' | 'GEOSEARCHSTORE',
): string {
  if (legacy) {
    return 'ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options'
  }
  return command === 'GEOSEARCHSTORE'
    ? 'ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options'
    : 'ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options'
}

/**
 * GEOSEARCH FROMMEMBER / GEORADIUSBYMEMBER of a missing member: valkey 9.0
 * names it, everything before says the zset member could not be decoded.
 */
export function geoMissingMember(member: string): string {
  return activeProfile === 'valkey-9.0'
    ? `ERR member ${member} does not exist`
    : 'ERR could not decode requested zset member'
}
