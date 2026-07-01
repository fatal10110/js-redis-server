import { geoaddCommand } from './geoadd'
import { geodistCommand } from './geodist'
import { georadiusCommand, georadiusRoCommand } from './georadius'
import {
  georadiusbymemberCommand,
  georadiusbymemberRoCommand,
} from './georadiusbymember'
import { geohashCommand } from './geohash'
import { geoposCommand } from './geopos'
import { geosearchCommand } from './geosearch'
import { geosearchstoreCommand } from './geosearchstore'

export const geoCommands = [
  geoaddCommand,
  geoposCommand,
  geodistCommand,
  geohashCommand,
  geosearchCommand,
  geosearchstoreCommand,
  georadiusCommand,
  georadiusRoCommand,
  georadiusbymemberCommand,
  georadiusbymemberRoCommand,
]

export {
  geoaddCommand,
  geoposCommand,
  geodistCommand,
  geohashCommand,
  geosearchCommand,
  geosearchstoreCommand,
  georadiusCommand,
  georadiusRoCommand,
  georadiusbymemberCommand,
  georadiusbymemberRoCommand,
}
