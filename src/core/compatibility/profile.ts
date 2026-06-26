import { FEATURE_GATES } from './feature-gates'

export type RedisFlavor = 'redis' | 'valkey'

export type VersionGate = { redis?: string; valkey?: string }

export type FeatureId =
  | 'expire.conditions'
  | 'set.get'
  | 'set.nx-get'
  | 'set.exat-pxat'
  | 'command.docs'
  | 'command.getkeysandflags'
  | 'client.setinfo'
  | 'pubsub.sharded'
  | 'cluster.multi-db'

export interface CompatibilityProfile {
  readonly flavor: RedisFlavor
  readonly version: string
  readonly versionNum: number
  has(feature: FeatureId): boolean
}

export type CompatibilitySpec =
  | CompatibilityProfile
  | { flavor?: RedisFlavor; version?: string }
  | 'redis-6.2'
  | 'redis-7.0'
  | 'redis-7.2'
  | 'redis-7.4'
  | 'redis-8.0'
  | 'valkey-8.0'
  | 'valkey-9.0'

const PRESETS: Record<
  Exclude<
    CompatibilitySpec,
    CompatibilityProfile | { flavor?: RedisFlavor; version?: string }
  >,
  { flavor: RedisFlavor; version: string }
> = {
  'redis-6.2': { flavor: 'redis', version: '6.2.14' },
  'redis-7.0': { flavor: 'redis', version: '7.0.15' },
  'redis-7.2': { flavor: 'redis', version: '7.2.4' },
  'redis-7.4': { flavor: 'redis', version: '7.4.4' },
  'redis-8.0': { flavor: 'redis', version: '8.0.0' },
  'valkey-8.0': { flavor: 'valkey', version: '8.0.0' },
  'valkey-9.0': { flavor: 'valkey', version: '9.0.0' },
}

const DEFAULT_SPEC = 'redis-8.0'

export function parseVersion(version: string): number {
  const [majorRaw, minorRaw = '0', patchRaw = '0'] = version.split('.')
  const major = parseVersionPart(majorRaw, version)
  const minor = parseVersionPart(minorRaw, version)
  const patch = parseVersionPart(patchRaw, version)
  return major * 10000 + minor * 100 + patch
}

export function resolveCompatibilityProfile(
  spec: CompatibilitySpec = DEFAULT_SPEC,
): CompatibilityProfile {
  if (typeof spec === 'object' && 'has' in spec) {
    return spec
  }

  if (typeof spec === 'string' && !(spec in PRESETS)) {
    throw new Error(`Unknown compatibility profile ${spec}`)
  }

  const resolved = typeof spec === 'string' ? PRESETS[spec] : spec
  const flavor = resolved.flavor ?? 'redis'
  const version = resolved.version ?? PRESETS[DEFAULT_SPEC].version
  const versionNum = parseVersion(version)

  return {
    flavor,
    version,
    versionNum,
    has: feature =>
      gateSatisfied(FEATURE_GATES[feature], { flavor, versionNum }),
  }
}

export function gateSatisfied(
  gate: VersionGate,
  profile: Pick<CompatibilityProfile, 'flavor' | 'versionNum'>,
): boolean {
  const minimumVersion = gate[profile.flavor]
  return (
    minimumVersion !== undefined &&
    profile.versionNum >= parseVersion(minimumVersion)
  )
}

function parseVersionPart(raw: string | undefined, version: string): number {
  if (raw === undefined || !/^\d+$/.test(raw)) {
    throw new Error(`Invalid compatibility version ${version}`)
  }
  return Number(raw)
}
