import { findClientById } from '../core/client-id'
import type { RedisClientSession } from '../core/redis-context'
import { RedisCommandError } from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { RedisMutationEvent } from './mutation-events'

/** CLIENT TRACKING ON's options, parsed (`REDIRECT 0` never gets here). */
export type ClientTrackingOptions = {
  /** The REDIRECT client id, `0` for none. */
  redirect: number
  bcast: boolean
  optin: boolean
  optout: boolean
  noloop: boolean
  prefixes: readonly Buffer[]
}

/** What CLIENT TRACKINGINFO, GETREDIR and CLIENT LIST report. */
export type ClientTrackingInfo = {
  on: boolean
  bcast: boolean
  optin: boolean
  optout: boolean
  noloop: boolean
  /** CLIENT CACHING YES / NO given for the next command. */
  caching: boolean
  brokenRedirect: boolean
  /** The REDIRECT id, `0` for none; meaningful only while `on`. */
  redirect: number
  /** BCAST prefixes, sorted bytewise; empty when not BCAST. */
  prefixes: Buffer[]
}

type ClientState = {
  on: boolean
  bcast: boolean
  optin: boolean
  optout: boolean
  noloop: boolean
  caching: boolean
  brokenRedirect: boolean
  redirect: number
  /** `client_tracking_prefixes`: set while BCAST, by prefix bytes. */
  prefixes: Map<string, Buffer> | null
  /** Unregisters the RESET / close hook that disables tracking. */
  release?: () => void
}

type BroadcastState = {
  prefix: Buffer
  clients: Set<RedisClientSession>
  /** Keys changed since the last broadcast, each with its last writer. */
  keys: Map<string, { key: Buffer; writer: object | undefined }>
}

type InvalidationPayload = readonly Buffer[] | null

const INVALIDATE = 'invalidate'
const TRACKING_CHANNEL = Buffer.from('__redis__:invalidate')

/**
 * Server-assisted client-side caching (CLIENT TRACKING), after real Redis'
 * tracking.c (redis@7.2.4; valkey 8/9 carry the same code).
 *
 * - Default mode remembers, per key name, the clients that read it with a
 *   readonly command ({@link rememberKeys}); the first change to the key
 *   (any database) sends each of them one `invalidate` message and forgets
 *   the key.
 * - BCAST mode remembers nothing per client: every changed key matching a
 *   registered prefix is batched, and once per event-loop turn each prefix's
 *   clients get the sorted keys in one message.
 * - FLUSHDB / FLUSHALL send every tracking client one null invalidation.
 *
 * Changes arrive from every database's mutation bus ({@link handle}), so
 * MULTI/EXEC, scripts and expiry are covered. An invalidation for the client
 * whose own command made the change waits until that command's reply is out
 * (`server.tracking_pending_keys`): {@link afterCommand} hands it over.
 */
export class ClientTrackingTable {
  private readonly states = new WeakMap<RedisClientSession, ClientState>()
  /** Clients with tracking on, in the order they turned it on. */
  private readonly active = new Set<RedisClientSession>()
  /** `TrackingTable`: key name -> the clients that read it. */
  private readonly table = new Map<
    string,
    { key: Buffer; clients: Set<RedisClientSession> }
  >()
  /** `PrefixTable`: BCAST prefix -> its clients and pending keys. */
  private readonly prefixTable = new Map<string, BroadcastState>()
  /** `tracking_pending_keys`, per client: `null` is a flush. */
  private readonly pending = new Map<RedisClientSession, (Buffer | null)[]>()
  private broadcastScheduled = false
  private flushBatch: { seen: boolean } | undefined

  constructor(
    private readonly connectedClients: () => readonly RedisClientSession[],
  ) {}

  info(session: RedisClientSession): ClientTrackingInfo {
    const state = this.states.get(session)
    return {
      on: state?.on ?? false,
      bcast: state?.bcast ?? false,
      optin: state?.optin ?? false,
      optout: state?.optout ?? false,
      noloop: state?.noloop ?? false,
      caching: state?.caching ?? false,
      brokenRedirect: state?.brokenRedirect ?? false,
      redirect: state?.redirect ?? 0,
      prefixes: state?.prefixes ? sortedBuffers(state.prefixes.values()) : [],
    }
  }

  /**
   * CLIENT TRACKING ON, after its options parsed: the mode checks in the
   * order networking.c makes them, then `enableTracking`.
   */
  enable(session: RedisClientSession, options: ClientTrackingOptions): void {
    const state = this.stateOf(session)
    if (!options.bcast && options.prefixes.length > 0) {
      throw new RedisCommandError(
        'PREFIX option requires BCAST mode to be enabled',
      )
    }
    if (state.on && state.bcast !== options.bcast) {
      throw new RedisCommandError(
        "You can't switch BCAST mode on/off before disabling tracking for " +
          'this client, and then re-enabling it with a different mode.',
      )
    }
    if (options.bcast && (options.optin || options.optout)) {
      throw new RedisCommandError(
        'OPTIN and OPTOUT are not compatible with BCAST',
      )
    }
    if (options.optin && options.optout) {
      throw new RedisCommandError(
        "You can't specify both OPTIN mode and OPTOUT mode",
      )
    }
    if ((options.optin && state.optout) || (options.optout && state.optin)) {
      throw new RedisCommandError(
        "You can't switch OPTIN/OPTOUT mode before disabling tracking for " +
          'this client, and then re-enabling it with a different mode.',
      )
    }
    if (options.bcast) {
      checkPrefixCollisions(state.prefixes, options.prefixes)
    }

    if (!state.on) {
      this.active.add(session)
      state.release = session.onReset(() => this.disable(session))
    }
    state.on = true
    state.brokenRedirect = false
    state.redirect = options.redirect
    state.optin = options.optin
    state.optout = options.optout
    state.noloop = options.noloop
    if (options.bcast) {
      state.bcast = true
      const prefixes =
        options.prefixes.length === 0 ? [Buffer.alloc(0)] : options.prefixes
      for (const prefix of prefixes) {
        this.enableBroadcastPrefix(session, state, prefix)
      }
    }
  }

  /** CLIENT TRACKING OFF, RESET and close (`disableTracking`). */
  disable(session: RedisClientSession): void {
    const state = this.states.get(session)
    if (!state) {
      return
    }
    if (state.bcast && state.prefixes) {
      for (const id of state.prefixes.keys()) {
        const broadcast = this.prefixTable.get(id)
        broadcast?.clients.delete(session)
        if (broadcast && broadcast.clients.size === 0) {
          this.prefixTable.delete(id)
        }
      }
      state.prefixes = null
    }
    if (state.on) {
      this.active.delete(session)
      state.on = false
      state.brokenRedirect = false
      state.bcast = false
      state.optin = false
      state.optout = false
      state.caching = false
      state.noloop = false
      const release = state.release
      state.release = undefined
      release?.()
    }
  }

  /** CLIENT CACHING YES | NO. */
  setCaching(session: RedisClientSession, mode: string): void {
    const state = this.states.get(session)
    if (!state?.on) {
      throw new RedisCommandError(
        'CLIENT CACHING can be called only when the client is in tracking ' +
          'mode with OPTIN or OPTOUT mode enabled',
      )
    }
    if (mode === 'yes') {
      if (!state.optin) {
        throw new RedisCommandError(
          'CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.',
        )
      }
    } else if (mode === 'no') {
      if (!state.optout) {
        throw new RedisCommandError(
          'CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.',
        )
      }
    } else {
      throw new RedisCommandError('syntax error')
    }
    state.caching = true
  }

  /**
   * `trackingRememberKeys`: `session` (the client that sent the command, the
   * caller of a script) read `keys` with a readonly command.
   */
  rememberKeys(session: RedisClientSession, keys: readonly Buffer[]): void {
    const state = this.states.get(session)
    if (!state?.on || state.bcast || keys.length === 0) {
      return
    }
    if ((state.optin && !state.caching) || (state.optout && state.caching)) {
      return
    }
    for (const key of keys) {
      const id = keyId(key)
      let entry = this.table.get(id)
      if (!entry) {
        entry = { key: Buffer.from(key), clients: new Set() }
        this.table.set(id, entry)
      }
      entry.clients.add(session)
    }
  }

  /** The listener on every database's mutation bus. */
  handle(event: RedisMutationEvent): void {
    switch (event.type) {
      case 'write':
      case 'delete':
      case 'expire':
      case 'persist':
      case 'evict':
        // BCAST NOLOOP compares the client handed to `signalModifiedKey`:
        // the script's own client for a script write, none for a key that
        // expired (redis@7.2.4:src/db.c:1687), the command's for a deadline
        // it gave in the past (valkey@9.0.0:src/db.c:1928-1936). Default mode
        // compares the caller.
        this.invalidateKey(
          event.key,
          event.client,
          event.viaScript || (event.type === 'evict' && !event.pastDeadline)
            ? undefined
            : event.client,
        )
        return
      case 'flush':
        if (this.flushBatch) {
          this.flushBatch.seen = true
          return
        }
        this.invalidateOnFlush(event.client, false)
        return
      default:
        return
    }
  }

  /**
   * Run `flush` (one or many database flushes) as one flush signal: FLUSHALL
   * empties every database but real Redis signals it once
   * (`signalFlushedDb(-1)`). `client` ran the flush; with `pushFirst` its own
   * null goes out before its reply instead of after (redis 7.4+ runs a sync
   * flush as a blocking async one, replying once it is done).
   */
  coalesceFlush(
    client: object | undefined,
    pushFirst: boolean,
    flush: () => void,
  ): void {
    if (this.flushBatch) {
      flush()
      return
    }
    const batch = { seen: false }
    this.flushBatch = batch
    try {
      flush()
    } finally {
      this.flushBatch = undefined
      if (batch.seen) {
        this.invalidateOnFlush(client, pushFirst)
      }
    }
  }

  /**
   * Call once `session`'s command is done: ends a CLIENT CACHING given for it
   * (except inside MULTI, or for a CLIENT command), and returns the delivery
   * of the invalidations its own command caused, to run after its reply.
   */
  afterCommand(
    session: RedisClientSession,
    clientCommand: boolean,
  ): (() => void) | undefined {
    const state = this.states.get(session)
    if (state?.caching && session.mode !== 'transaction' && !clientCommand) {
      state.caching = false
    }
    const pending = this.pending.get(session)
    if (!pending) {
      return undefined
    }
    this.pending.delete(session)
    return () => {
      for (const payload of pending) {
        // Past the reply: skip the holds that would queue them behind the
        // command's other held pushes.
        this.send(session, payload === null ? null : [payload], true)
      }
    }
  }

  private stateOf(session: RedisClientSession): ClientState {
    let state = this.states.get(session)
    if (!state) {
      state = {
        on: false,
        bcast: false,
        optin: false,
        optout: false,
        noloop: false,
        caching: false,
        brokenRedirect: false,
        redirect: 0,
        prefixes: null,
      }
      this.states.set(session, state)
    }
    return state
  }

  private enableBroadcastPrefix(
    session: RedisClientSession,
    state: ClientState,
    prefix: Buffer,
  ): void {
    const id = keyId(prefix)
    let broadcast = this.prefixTable.get(id)
    if (!broadcast) {
      broadcast = {
        prefix: Buffer.from(prefix),
        clients: new Set(),
        keys: new Map(),
      }
      this.prefixTable.set(id, broadcast)
    }
    if (broadcast.clients.has(session)) {
      return
    }
    broadcast.clients.add(session)
    state.prefixes ??= new Map()
    state.prefixes.set(id, Buffer.from(prefix))
  }

  /** `trackingInvalidateKey`. */
  private invalidateKey(
    key: Buffer,
    writer: object | undefined,
    broadcastWriter: object | undefined,
  ): void {
    if (this.prefixTable.size > 0) {
      this.rememberKeyToBroadcast(key, broadcastWriter)
    }
    const id = keyId(key)
    const entry = this.table.get(id)
    if (!entry) {
      return
    }
    this.table.delete(id)
    for (const target of entry.clients) {
      const state = this.states.get(target)
      if (!state?.on || state.bcast) {
        continue
      }
      if (state.noloop && target === writer) {
        continue
      }
      if (target === writer) {
        this.addPending(target, entry.key)
      } else {
        this.send(target, [entry.key])
      }
    }
  }

  /** `trackingInvalidateKeysOnFlush`. */
  private invalidateOnFlush(
    flusher: object | undefined,
    pushFirst: boolean,
  ): void {
    for (const session of Array.from(this.active)) {
      if (session === flusher && !pushFirst) {
        this.addPending(session, null)
      } else {
        this.send(session, null, session === flusher)
      }
    }
    this.table.clear()
  }

  private addPending(session: RedisClientSession, payload: Buffer | null) {
    let pending = this.pending.get(session)
    if (!pending) {
      pending = []
      this.pending.set(session, pending)
    }
    pending.push(payload)
  }

  /** `trackingRememberKeyToBroadcast`. */
  private rememberKeyToBroadcast(key: Buffer, writer: object | undefined) {
    for (const broadcast of this.prefixTable.values()) {
      const { prefix } = broadcast
      if (prefix.length > key.length) {
        continue
      }
      if (prefix.length > 0 && !key.subarray(0, prefix.length).equals(prefix)) {
        continue
      }
      broadcast.keys.set(keyId(key), { key: Buffer.from(key), writer })
      this.scheduleBroadcast()
    }
  }

  private scheduleBroadcast(): void {
    if (this.broadcastScheduled) {
      return
    }
    this.broadcastScheduled = true
    // Real Redis broadcasts in beforeSleep, after the event loop turn's
    // replies are written.
    setImmediate(() => {
      this.broadcastScheduled = false
      this.broadcast()
    })
  }

  /** `trackingBroadcastInvalidationMessages`. */
  private broadcast(): void {
    const states = Array.from(this.prefixTable.values()).sort((a, b) =>
      Buffer.compare(a.prefix, b.prefix),
    )
    for (const broadcast of states) {
      if (broadcast.keys.size === 0) {
        continue
      }
      const changed = Array.from(broadcast.keys.values()).sort((a, b) =>
        Buffer.compare(a.key, b.key),
      )
      broadcast.keys = new Map()
      const all = changed.map(entry => entry.key)
      for (const session of Array.from(broadcast.clients)) {
        const keys = this.states.get(session)?.noloop
          ? changed
              .filter(entry => entry.writer !== session)
              .map(entry => entry.key)
          : all
        if (keys.length > 0) {
          this.send(session, keys)
        }
      }
    }
  }

  /**
   * `sendTrackingMessage`: deliver to `tracker`, or to its REDIRECT client.
   * A RESP3 connection gets an `invalidate` push; a RESP2 redirect target
   * subscribed to anything gets it as a `__redis__:invalidate` message; any
   * other RESP2 connection gets nothing. A flush's null is spelled in the
   * tracker's protocol, whoever receives it.
   */
  private send(
    tracker: RedisClientSession,
    keys: InvalidationPayload,
    beforeReply = false,
  ): void {
    const state = this.states.get(tracker)
    const trackerProtocol = tracker.protocolVersion
    let target = tracker
    let redirected = false
    if (state && state.redirect !== 0) {
      const redirect = findClientById(this.connectedClients(), state.redirect)
      if (!redirect) {
        state.brokenRedirect = true
        if (trackerProtocol === 3) {
          tracker.enqueuePush(
            RedisResult.create(
              RedisValue.push('tracking-redir-broken', [
                RedisValue.integer(state.redirect),
              ]),
            ),
          )
        }
        return
      }
      target = redirect
      redirected = true
    }

    const payload = encodePayload(keys, trackerProtocol)
    if (target.protocolVersion === 3) {
      const header = Buffer.from(`>2\r\n$10\r\n${INVALIDATE}\r\n`)
      target.enqueuePush(
        RedisResult.preEncoded(
          RedisValue.push(INVALIDATE, [payload.value]),
          Buffer.concat([header, payload.bytes]),
        ),
        beforeReply && target === tracker,
      )
      return
    }
    if (redirected && target.pubsubSubscriptionCount > 0) {
      const header = Buffer.from(
        `*3\r\n$7\r\nmessage\r\n$${TRACKING_CHANNEL.length}\r\n${TRACKING_CHANNEL.toString()}\r\n`,
      )
      target.enqueuePush(
        RedisResult.preEncoded(
          RedisValue.push('message', [
            RedisValue.bulkString(Buffer.from(TRACKING_CHANNEL)),
            payload.value,
          ]),
          Buffer.concat([header, payload.bytes]),
        ),
      )
    }
  }
}

/** `checkPrefixCollisionsOrReply`. */
function checkPrefixCollisions(
  existing: ReadonlyMap<string, Buffer> | null,
  prefixes: readonly Buffer[],
): void {
  for (let i = 0; i < prefixes.length; i++) {
    // The rax walks the client's prefixes bytewise.
    for (const current of existing ? sortedBuffers(existing.values()) : []) {
      if (sharesPrefix(current, prefixes[i])) {
        throw new RedisCommandError(
          Buffer.concat([
            Buffer.from("Prefix '"),
            prefixes[i],
            Buffer.from("' overlaps with an existing prefix '"),
            current,
            Buffer.from("'. Prefixes for a single client must not overlap."),
          ]),
        )
      }
    }
    for (let j = i + 1; j < prefixes.length; j++) {
      if (sharesPrefix(prefixes[i], prefixes[j])) {
        throw new RedisCommandError(
          Buffer.concat([
            Buffer.from("Prefix '"),
            prefixes[i],
            Buffer.from("' overlaps with another provided prefix '"),
            prefixes[j],
            Buffer.from("'. Prefixes for a single client must not overlap."),
          ]),
        )
      }
    }
  }
}

/** `stringCheckPrefix`: one is a prefix of the other. */
function sharesPrefix(a: Buffer, b: Buffer): boolean {
  const length = Math.min(a.length, b.length)
  return a.subarray(0, length).equals(b.subarray(0, length))
}

function encodePayload(
  keys: InvalidationPayload,
  trackerProtocol: number,
): { value: RedisValue; bytes: Buffer } {
  if (keys === null) {
    return {
      value: RedisValue.null(),
      bytes: Buffer.from(trackerProtocol === 3 ? '_\r\n' : '$-1\r\n'),
    }
  }
  const parts: Buffer[] = [Buffer.from(`*${keys.length}\r\n`)]
  for (const key of keys) {
    parts.push(Buffer.from(`$${key.length}\r\n`), key, Buffer.from('\r\n'))
  }
  return {
    value: RedisValue.array(
      keys.map(key => RedisValue.bulkString(Buffer.from(key))),
    ),
    bytes: Buffer.concat(parts),
  }
}

function sortedBuffers(values: Iterable<Buffer>): Buffer[] {
  return Array.from(values, value => Buffer.from(value)).sort(Buffer.compare)
}

function keyId(key: Buffer): string {
  return key.toString('latin1')
}
