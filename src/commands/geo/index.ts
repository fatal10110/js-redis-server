import { geoaddCommand } from './geoadd'
import { geodistCommand } from './geodist'
import { geohashCommand } from './geohash'
import { geoposCommand } from './geopos'

export const geoCommands = [
  geoaddCommand,
  geoposCommand,
  geodistCommand,
  geohashCommand,
]

export { geoaddCommand, geoposCommand, geodistCommand, geohashCommand }
