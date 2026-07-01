import { ClientSession } from '../client-session'
import type { CommandExecutor } from '../command-executor'
import type { RespEncodeOptions } from '../resp-encoder'
import type { Logger } from '../../logger'
import type { RedisClusterNodeRole, RedisServerState } from '../../state'
import type { ConnectionTransport } from './connection-transport'
import { Resp2SessionAdapter } from './resp2/session-adapter'

export type AttachSessionOptions = {
  state: RedisServerState
  executor: CommandExecutor
  nodeRole?: RedisClusterNodeRole
  logger?: Pick<Logger, 'error'>
  encoder?: RespEncodeOptions
  clientAddress?: string
}

export type AttachedSession = {
  session: ClientSession
  adapter: Resp2SessionAdapter
  /** Resolves once the adapter's read/write loop finishes (transport closed). */
  done: Promise<void>
  /** Tear down the transport (which aborts the adapter and closes the session). */
  close(): void
}

/**
 * Wire a {@link ConnectionTransport} to a fresh {@link ClientSession} driven by
 * a {@link Resp2SessionAdapter}, kicking off the adapter's run loop.
 *
 * This is the transport-agnostic core extracted from `Resp2Server.handleConnection`:
 * the socket-backed server and the in-memory virtual connection share it, so the
 * RESP framing / session lifecycle never diverges between them. The returned
 * `done` promise settles when the adapter loop ends; `close()` closes the
 * transport, which aborts the adapter and (via the adapter's `finally`) the
 * session.
 */
export function attachSession(
  transport: ConnectionTransport,
  opts: AttachSessionOptions,
): AttachedSession {
  const session = new ClientSession({
    server: opts.state,
    executor: opts.executor,
    nodeRole: opts.nodeRole,
    clientAddress: opts.clientAddress,
    closeConnection: reason => transport.close(reason ?? 'client disconnected'),
  })

  const adapter = new Resp2SessionAdapter({
    transport,
    session,
    logger: opts.logger,
    encoder: opts.encoder,
  })

  const done = adapter.run().catch(err => opts.logger?.error(err))

  return {
    session,
    adapter,
    done,
    close: () => transport.close('attach-session closed'),
  }
}
