export function formatHostPort(host: string, port: number): string {
  return `${formatHost(host)}:${port}`
}

export function formatSocketAddressParts(
  address: string | undefined,
  port: number | undefined,
): string | undefined {
  const host = normalizeSocketAddress(address)
  if (!host || port === undefined) {
    return host
  }

  return formatHostPort(host, port)
}

function normalizeSocketAddress(
  address: string | undefined,
): string | undefined {
  if (address?.startsWith('::ffff:')) {
    return address.slice('::ffff:'.length)
  }

  return address
}

function formatHost(host: string): string {
  if (host.includes(':') && !host.startsWith('[')) {
    return `[${host}]`
  }

  return host
}
