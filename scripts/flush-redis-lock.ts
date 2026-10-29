/**
 * The run lock `scripts/flush-redis.ts` takes on a real-backend stack before
 * it flushes anything (#542), kept free of I/O so it can be unit-tested.
 *
 * `test:integration:real*` starts with a FLUSHALL of every master. Two runs
 * against one stack (two worktrees, checkouts or agents that did not start a
 * private stack) used to wipe each other's keys mid-test without a word
 * (#497). The second run now refuses to flush while another one holds the
 * stack.
 *
 * The lock is a named client connection, not a key:
 *
 *  - a key does not survive the suites themselves: they run FLUSHALL and
 *    FLUSHDB mid-run (flush-async-sync, randomkey). A client name is
 *    connection state and outlives any flush;
 *  - a crashed run releases it at once, with no TTL to wait out. The kernel
 *    closes the dead process's sockets, and the server drops the name with
 *    the connection. A run still in progress (even a hung one) keeps it;
 *  - it needs no keepalive traffic, which MONITOR tests would see as stray
 *    lines.
 *
 * Every run names one connection on every endpoint it is about to flush
 * (`runLockName`), and only then lists the clients of each one
 * (`otherRunHolders`). A run that sees another run's name anywhere refuses.
 * Because a run names itself everywhere before it lists anything, of two runs
 * that share an endpoint the one that lists it later always sees the other.
 * At most one of them goes on to flush; when they start at the same instant
 * both may refuse, which is loud and safe.
 */

/** Prefix of every lock connection's CLIENT SETNAME. */
export const RUN_LOCK_PREFIX = 'js-redis-server-test-run:'

/**
 * Keep only characters CLIENT SETNAME accepts and that cannot be mistaken for
 * a CLIENT LIST field separator: real Redis rejects spaces, newlines and
 * anything outside `!`..`~`.
 */
function nameSafe(text: string): string {
  return text.replace(/[^A-Za-z0-9._-]/g, '_') || '_'
}

/**
 * The client name a run holds the stack under. It says where the run comes
 * from (host and pid), so the refusal can tell the user which run to wait for;
 * the nonce keeps two runs apart when a pid is reused.
 */
export function runLockName(
  hostname: string,
  pid: number,
  nonce: string,
): string {
  return `${RUN_LOCK_PREFIX}${nameSafe(hostname)}:${pid}:${nameSafe(nonce)}`
}

export type ClientEntry = {
  id: string
  addr: string
  name: string
  /** Seconds since the connection was made, when CLIENT LIST reports it. */
  age: number | undefined
}

/**
 * Parse CLIENT LIST: one client per line, `key=value` fields separated by
 * single spaces. Names cannot contain spaces, so splitting on them is exact.
 */
export function parseClientList(text: string): ClientEntry[] {
  return text
    .split('\n')
    .map(line => line.trim())
    .filter(line => line !== '')
    .map(line => {
      const fields = new Map<string, string>()
      for (const field of line.split(' ')) {
        const separator = field.indexOf('=')
        if (separator > 0) {
          fields.set(field.slice(0, separator), field.slice(separator + 1))
        }
      }
      const age = Number(fields.get('age'))
      return {
        id: fields.get('id') ?? '',
        addr: fields.get('addr') ?? '',
        name: fields.get('name') ?? '',
        age: fields.has('age') && Number.isFinite(age) ? age : undefined,
      }
    })
}

/** The clients in a CLIENT LIST reply that hold the lock for another run. */
export function otherRunHolders(
  clientList: string,
  ownName: string,
): ClientEntry[] {
  return parseClientList(clientList).filter(
    client =>
      client.name.startsWith(RUN_LOCK_PREFIX) && client.name !== ownName,
  )
}

/** One holder, as the refusal names it. */
export function describeHolder(holder: ClientEntry): string {
  const age = holder.age === undefined ? '' : `, connected ${holder.age}s ago`
  return `held by ${holder.name} (from ${holder.addr || 'an unknown address'}${age})`
}

export const USAGE =
  'usage: flush-redis.ts [-- <command> [args...]]\n' +
  '  without a command: take the run lock, flush, verify, release\n' +
  '  with a command: take the run lock, flush, verify, then run the command ' +
  'and hold the lock until it exits'

/**
 * The command to run while holding the lock, or null to flush and release.
 * Everything after `--` is the command, passed to it verbatim; nothing else
 * is accepted, so a typo cannot silently turn into "flush only".
 */
export function parseCommandLine(argv: readonly string[]): string[] | null {
  if (argv.length === 0) {
    return null
  }
  if (argv[0] !== '--' || argv.length === 1) {
    throw new Error(USAGE)
  }
  return argv.slice(1)
}

/**
 * The exit code that reports a child's end: its own code, or 128 + the signal
 * number when a signal killed it, the way a shell reports it.
 */
export function exitCodeFor(
  code: number | null,
  signal: NodeJS.Signals | null,
  signals: Readonly<Record<string, number>>,
): number {
  if (code !== null) {
    return code
  }
  const number = signal === null ? undefined : signals[signal]
  return number === undefined ? 1 : 128 + number
}
