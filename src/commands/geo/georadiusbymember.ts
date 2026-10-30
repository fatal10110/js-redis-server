import { defineCommand } from '../../core/command-definition'
import { georadiusGetKeys } from '../../core/key-specs'
import { t, type ParseContext } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import { array, integer } from '../helpers'
import {
  buildSearchReply,
  buildStoreMembers,
  collectMatches,
  deferGeoParseError,
  georadiusRawKeys,
  orderAndLimit,
  parseByRadius,
  parseGeoRadiusFlags,
  resolveCenterFromMember,
  type GeoBy,
  type GeoCenter,
  type GeoRadiusFlags,
} from './search-core'

type GeoRadiusByMemberArgs = {
  key: Buffer
  member: Buffer
  radiusTok: Buffer
  unitTok: Buffer
  // The option tail, or the error it raised. Real Redis reports that error
  // only after the WRONGTYPE check, the member lookup and the radius parse.
  flags: GeoRadiusFlags | Error
  // When the options failed: the keys georadiusGetKeys() finds in them.
  rawKeys?: Buffer[]
}

function createGeoRadiusByMemberSchema(allowStore: boolean) {
  return t.custom<GeoRadiusByMemberArgs>(
    { min: 4, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const member = input[index + 1]
      const radiusTok = input[index + 2]
      const unitTok = input[index + 3]
      if (!key || !member || !radiusTok || !unitTok) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let flags: GeoRadiusFlags | Error
      let rawKeys: Buffer[] | undefined
      try {
        flags = parseGeoRadiusFlags(
          input,
          index + 4,
          allowStore,
          ctx.profile,
        )[0]
      } catch (err) {
        flags = deferGeoParseError(err)
        rawKeys = georadiusRawKeys(ctx.commandName, input)
      }

      return {
        value: { key, member, radiusTok, unitTok, flags, rawKeys },
        nextIndex: input.length,
      }
    },
  )
}

// Real georadiusGeneric() order: WRONGTYPE, then (only when the key exists)
// the member lookup and the radius / unit, then the options. A missing key
// skips the member and the radius / unit entirely and replies `*0`, or
// deletes the STORE destination and replies `:0`.
function executeGeoRadiusByMember(
  args: GeoRadiusByMemberArgs,
  ctx: RedisExecutionContext,
) {
  const profile = ctx.server.profile
  const zset = ctx.db.getSortedSet(args.key)
  let center: GeoCenter | undefined
  let by: GeoBy | undefined
  if (zset) {
    center = resolveCenterFromMember(zset, args.member, profile)
    by = parseByRadius(args.radiusTok, args.unitTok, profile)
  }
  if (args.flags instanceof Error) throw args.flags
  const flags = args.flags
  if (!center || !by) {
    const dest = flags.store ?? flags.storeDist
    if (!dest) return array([])
    ctx.db.delete(dest)
    return integer(0)
  }

  const matches = collectMatches(zset, center, by, profile)
  const ordered = orderAndLimit(matches, {
    order: flags.order,
    count: flags.count,
  })

  const dest = flags.store ?? flags.storeDist
  if (dest) {
    const members = buildStoreMembers(
      ordered,
      flags.storeDist !== undefined,
      by.unit,
    )
    if (members.size === 0) {
      ctx.db.delete(dest)
      return integer(0)
    }
    // One write replacing whatever the destination held, published as
    // `georadiusstore` like real Redis — not `del` followed by the command.
    ctx.db.withOrigin('georadiusstore').set(dest, { type: 'zset', members })
    return integer(members.size)
  }

  return buildSearchReply(
    ordered,
    {
      withCoord: flags.withCoord,
      withDist: flags.withDist,
      withHash: flags.withHash,
    },
    by.unit,
    profile,
  )
}

export const georadiusbymemberCommand = defineCommand({
  name: 'georadiusbymember',
  rawKeys: georadiusGetKeys,
  schema: createGeoRadiusByMemberSchema(true),
  flags: ['write', 'denyoom'],
  keys: args => {
    if (args.flags instanceof Error) return args.rawKeys ?? [args.key]
    const dest = args.flags.store ?? args.flags.storeDist
    return dest ? [args.key, dest] : [args.key]
  },
  execute: executeGeoRadiusByMember,
})

export const georadiusbymemberRoCommand = defineCommand({
  name: 'georadiusbymember_ro',
  schema: createGeoRadiusByMemberSchema(false),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: executeGeoRadiusByMember,
})
