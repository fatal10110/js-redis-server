import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import { integer } from '../helpers'
import {
  buildSearchReply,
  buildStoreMembers,
  collectMatches,
  orderAndLimit,
  parseByRadius,
  parseGeoRadiusFlags,
  resolveCenterFromMember,
  type GeoBy,
  type GeoRadiusFlags,
} from './search-core'

type GeoRadiusByMemberArgs = GeoRadiusFlags & {
  key: Buffer
  member: Buffer
  by: GeoBy
}

function createGeoRadiusByMemberSchema(allowStore: boolean) {
  return t.custom<GeoRadiusByMemberArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const member = input[index + 1]
      const radiusTok = input[index + 2]
      const unitTok = input[index + 3]
      if (!key || !member || !radiusTok || !unitTok) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const by = parseByRadius(radiusTok, unitTok)
      const [flags, nextIndex] = parseGeoRadiusFlags(
        input,
        index + 4,
        allowStore,
      )

      return { value: { key, member, by, ...flags }, nextIndex }
    },
  )
}

function executeGeoRadiusByMember(
  args: GeoRadiusByMemberArgs,
  ctx: RedisExecutionContext,
) {
  const zset = ctx.db.getSortedSet(args.key)
  const center = resolveCenterFromMember(zset, args.member)
  const matches = collectMatches(zset, center, args.by)
  const ordered = orderAndLimit(matches, {
    order: args.order,
    count: args.count,
  })

  const dest = args.store ?? args.storeDist
  if (dest) {
    const members = buildStoreMembers(
      ordered,
      args.storeDist !== undefined,
      args.by.unit,
    )
    if (members.size === 0) {
      ctx.db.delete(dest)
      return integer(0)
    }
    ctx.db.delete(dest)
    ctx.db.updateSortedSet(dest, destZset => {
      destZset.replaceMembers(members, { forceDirty: true })
    })
    return integer(members.size)
  }

  return buildSearchReply(
    ordered,
    {
      withCoord: args.withCoord,
      withDist: args.withDist,
      withHash: args.withHash,
    },
    args.by.unit,
  )
}

export const georadiusbymemberCommand = defineCommand({
  name: 'georadiusbymember',
  schema: createGeoRadiusByMemberSchema(true),
  flags: ['write', 'denyoom'],
  keys: args =>
    (args.store ?? args.storeDist)
      ? [args.key, (args.store ?? args.storeDist)!]
      : [args.key],
  execute: executeGeoRadiusByMember,
})

export const georadiusbymemberRoCommand = defineCommand({
  name: 'georadiusbymember_ro',
  schema: createGeoRadiusByMemberSchema(false),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: executeGeoRadiusByMember,
})
