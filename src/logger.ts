export interface Logger {
  info(msg: unknown, metadata?: Record<string, unknown>): void
  error(msg: unknown, metadata?: Record<string, unknown>): void
  debug(msg: unknown, metadata?: Record<string, unknown>): void
}
