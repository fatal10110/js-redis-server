import type { CompatibilityProfile } from './compatibility'
import { RedisCommandError, UnknownSubcommandError } from './redis-error'

// The two container-error templates, kept in src/core so that
// `CommandExecutor.plan()` (the 7.0+ subcommand lookup) and the containers
// themselves (src/commands, re-exported from src/commands/helpers.ts) build
// the same bytes from one place.

/**
 * Real Redis renders the echoed subcommand with `%.128s` on the 7.0+ template,
 * so a longer name is cut at 128 *bytes*. Verified exact against 7.0.15 and
 * 8.0.6: 127 and 128 come back whole, 129 and 300 are both cut to 128 — and
 * when the cut lands inside a multi-byte character the partial byte is emitted
 * raw (`'A'.repeat(127) + 'é'` replies with 128 bytes ending in a bare
 * 0xc3). That is why the cut is taken on the Buffer and the result never round
 * trips through a string. Redis 6.2 has no truncation at all — a 300-byte name
 * comes back whole.
 */
const SUBCOMMAND_ECHO_LIMIT = 128

/**
 * The reply real Redis sends when a container command is given a subcommand it
 * does not recognize — including a subcommand this server gates off on older
 * profiles, which real Redis of that vintage simply did not have.
 *
 * Redis 7.0 moved container commands into the command table, which changed both
 * the template and the echo model. Captured from real servers:
 *
 * ```
 * 6.2.24  CONFIG BOGUS -> Unknown subcommand or wrong number of arguments for 'BOGUS'. Try CONFIG HELP.
 * 7.0.15  CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * 8.0.6   CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * ```
 *
 * `container` is the upper-case parent name as it appears in the
 * `Try ... HELP.` suffix; `subcommand` is echoed with the bytes the client
 * sent, casing and all.
 *
 * On 7.0+ profiles `CommandExecutor.plan()` raises this for a subcommand the
 * real command table lacks, before the container runs (#435, #436, #439); the
 * containers raise it themselves on 6.2 and for real subcommands this server
 * does not implement. Both go through here, so the two cannot drift apart.
 */
export function unknownSubcommandError(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
): UnknownSubcommandError {
  return new UnknownSubcommandError(
    subcommandErrorBody(container, subcommand, profile, 'unknown'),
  )
}

/**
 * Real Redis' `addReplySubcommandSyntaxError` — the reply a container command
 * builds itself when a *known* subcommand is given arguments it cannot use, as
 * opposed to the dispatch-level {@link unknownSubcommandError}. Same 7.0 case
 * flip, but the wording keeps the `or wrong number of arguments` clause and
 * there is no `%.128s` truncation. Captured from real servers:
 *
 * ```
 * 6.2.24  PUBSUB CHANNELS a b -> Unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.
 * 8.0.6   PUBSUB CHANNELS a b -> unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.
 * ```
 */
export function subcommandSyntaxError(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
): RedisCommandError {
  return new RedisCommandError(
    subcommandErrorBody(container, subcommand, profile, 'syntax'),
  )
}

/**
 * Both templates in one place, because on 6.2 they *are* one template: before
 * container commands entered the command table every one of these replies came
 * out of `addReplySubcommandSyntaxError`.
 *
 * The body is built as a Buffer rather than a string so the echoed name
 * survives byte for byte — real 8.0.6 answers `CONFIG \xff\xfe\xfd` with those
 * three bytes, where a UTF-8 decode would turn each into U+FFFD.
 */
function subcommandErrorBody(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
  kind: 'unknown' | 'syntax',
): Buffer {
  const raw = Buffer.isBuffer(subcommand) ? subcommand : Buffer.from(subcommand)
  const echoed = asCString(raw)

  if (!profile.has('error.unknown-subcommand-wording')) {
    return subcommandBody(
      'Unknown subcommand or wrong number of arguments for',
      echoed,
      container,
    )
  }

  if (kind === 'syntax') {
    return subcommandBody(
      'unknown subcommand or wrong number of arguments for',
      echoed,
      container,
    )
  }

  return subcommandBody(
    'unknown subcommand',
    echoed.subarray(0, SUBCOMMAND_ECHO_LIMIT),
    container,
  )
}

/**
 * The prefix up to the first NUL. Both templates render the echoed name with a
 * `%s`-family conversion over a C string, so the echo stops at the first NUL
 * byte — and that cut happens *before* `%.128s` counts its 128. Captured from
 * real servers:
 *
 * ```
 * 8.0.6   CONFIG 'AA\0BB'              -> unknown subcommand 'AA'. Try CONFIG HELP.
 * 8.0.6   CONFIG 'A'*100 + \0 + 'A'*100 -> echoes 100, not 128
 * 8.0.6   CONFIG '\0' + 'A'*10          -> echoes nothing at all
 * 8.0.6   CONFIG 'A'*200 + \0 + 'A'*200 -> echoes 128 (NUL cut, then %.128s)
 * 6.2.24  CONFIG 'A'*200 + \0 + 'A'*200 -> echoes 200 (NUL cut, no length cut)
 * ```
 *
 * So 6.2 truncates at a NUL even though it does not truncate by length, which
 * is why this runs ahead of the profile branch rather than inside it. It also
 * keeps a raw NUL out of a `-ERR ...\r\n` simple-error frame, a body real Redis
 * has no way to produce.
 */
function asCString(value: Buffer): Buffer {
  const nul = value.indexOf(0)
  return nul === -1 ? value : value.subarray(0, nul)
}

function subcommandBody(
  lead: string,
  echoed: Buffer,
  container: string,
): Buffer {
  return Buffer.concat([
    Buffer.from(`${lead} '`),
    echoed,
    Buffer.from(`'. Try ${container} HELP.`),
  ])
}
