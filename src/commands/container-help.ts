import type { CompatibilityProfile } from '../core/compatibility'

// The HELP text of the container commands, as real Redis / Valkey print it
// (#491). Each function returns the container's own lines; `helpReply` adds
// the `HELP` entry and its per-version footer. The text is the real servers'
// even where it advertises something this server does not implement (CLIENT
// CAPA, the Valkey CLIENT LIST filters, ...). Captured from redis-server
// 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0, 8.1 and 9.0.0;
// pinned by tests-integration/compatibility/container-help-gates.test.ts.

const HEADER = (container: string) =>
  `${container} <subcommand> [<arg> [value] [opt] ...]. Subcommands are:`

export function configHelpLines(): string[] {
  return [
    HEADER('CONFIG'),
    'GET <pattern>',
    '    Return parameters matching the glob-like <pattern> and their values.',
    'SET <directive> <value>',
    '    Set the configuration <directive> to <value>.',
    'RESETSTAT',
    '    Reset statistics reported by the INFO command.',
    'REWRITE',
    '    Rewrite the configuration file.',
  ]
}

export function pubsubHelpLines(profile: CompatibilityProfile): string[] {
  const lines = [
    HEADER('PUBSUB'),
    'CHANNELS [<pattern>]',
    "    Return the currently active channels matching a <pattern> (default: '*').",
    'NUMPAT',
    '    Return number of subscriptions to patterns.',
    'NUMSUB [<channel> ...]',
    '    Return the number of subscribers for the specified channels, excluding',
    '    pattern subscriptions(default: no channels).',
  ]
  if (profile.has('pubsub.sharded')) {
    lines.push(
      'SHARDCHANNELS [<pattern>]',
      "    Return the currently active shard level channels matching a <pattern> (default: '*').",
      'SHARDNUMSUB [<shardchannel> ...]',
      '    Return the number of subscribers for the specified shard level channel(s)',
    )
  }
  return lines
}

export function slowlogHelpLines(profile: CompatibilityProfile): string[] {
  return [
    HEADER('SLOWLOG'),
    'GET [<count>]',
    ...(profile.has('help.slowlog-get-all')
      ? [
          '    Return top <count> entries from the slowlog (default: 10, -1 mean all).',
          '    Entries are made of:',
        ]
      : [
          '    Return top <count> entries from the slowlog (default: 10). Entries are',
          '    made of:',
        ]),
    '    id, timestamp, time in microseconds, arguments array, client IP and port,',
    '    client name',
    'LEN',
    '    Return the length of the slowlog.',
    'RESET',
    '    Reset the slowlog.',
  ]
}

export function aclHelpLines(profile: CompatibilityProfile): string[] {
  const rules = profile.has('help.acl-setuser-rules')
  return [
    HEADER('ACL'),
    'CAT [<category>]',
    '    List all commands that belong to <category>, or all command categories',
    '    when no category is specified.',
    'DELUSER <username> [<username> ...]',
    '    Delete a list of users.',
    ...(profile.has('acl.dryrun')
      ? [
          'DRYRUN <username> <command> [<arg> ...]',
          '    Returns whether the user can execute the given command without executing the command.',
        ]
      : []),
    'GETUSER <username>',
    "    Get the user's details.",
    'GENPASS [<bits>]',
    '    Generate a secure 256-bit user password. The optional `bits` argument can',
    '    be used to specify a different size.',
    'LIST',
    '    Show users details in config file format.',
    'LOAD',
    '    Reload users from the ACL file.',
    'LOG [<count> | RESET]',
    '    Show the ACL log entries.',
    'SAVE',
    '    Save the current config to the ACL file.',
    rules
      ? 'SETUSER <username> <rule> [<rule> ...]'
      : 'SETUSER <username> <attribute> [<attribute> ...]',
    rules
      ? '    Create or modify a user with the specified rules.'
      : '    Create or modify a user with the specified attributes.',
    'USERS',
    '    List all the registered usernames.',
    'WHOAMI',
    '    Return the current connection username.',
  ]
}

export function scriptHelpLines(profile: CompatibilityProfile): string[] {
  const valkey = profile.has('help.script-valkey-text')
  return [
    HEADER('SCRIPT'),
    'DEBUG (YES|SYNC|NO)',
    '    Set the debug mode for subsequent scripts executed.',
    'EXISTS <sha1> [<sha1> ...]',
    '    Return information about the existence of the scripts in the script cache.',
    'FLUSH [ASYNC|SYNC]',
    '    Flush the Lua scripts cache. Very dangerous on replicas.',
    ...(valkey
      ? [
          '    When called without the optional mode argument, the behavior is determined',
          '     by the lazyfree-lazy-user-flush configuration directive. Valid modes are:',
        ]
      : [
          '    When called without the optional mode argument, the behavior is determined by the',
          '    lazyfree-lazy-user-flush configuration directive. Valid modes are:',
        ]),
    '    * ASYNC: Asynchronously flush the scripts cache.',
    '    * SYNC: Synchronously flush the scripts cache.',
    'KILL',
    '    Kill the currently executing Lua script.',
    'LOAD <script>',
    '    Load a script into the scripts cache without executing it.',
    ...(valkey
      ? ['SHOW <sha1>', '    Show a script from the scripts cache.']
      : []),
  ]
}

export function functionHelpLines(profile: CompatibilityProfile): string[] {
  return [
    HEADER('FUNCTION'),
    'LOAD [REPLACE] <FUNCTION CODE>',
    '    Create a new library with the given library name and code.',
    'DELETE <LIBRARY NAME>',
    '    Delete the given library.',
    'LIST [LIBRARYNAME PATTERN] [WITHCODE]',
    '    Return general information on all the libraries:',
    '    * Library name',
    '    * The engine used to run the Library',
    ...(profile.has('help.function-no-library-description')
      ? []
      : ['    * Library description']),
    '    * Functions list',
    '    * Library code (if WITHCODE is given)',
    '    It also possible to get only function that matches a pattern using LIBRARYNAME argument.',
    'STATS',
    '    Return information about the current function running:',
    '    * Function name',
    '    * Command used to run the function',
    '    * Duration in MS that the function is running',
    '    If no function is running, return nil',
    '    In addition, returns a list of available engines.',
    'KILL',
    '    Kill the current running function.',
    'FLUSH [ASYNC|SYNC]',
    '    Delete all the libraries.',
    '    When called without the optional mode argument, the behavior is determined by the',
    '    lazyfree-lazy-user-flush configuration directive. Valid modes are:',
    '    * ASYNC: Asynchronously flush the libraries.',
    '    * SYNC: Synchronously flush the libraries.',
    'DUMP',
    '    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command',
    'RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]',
    '    Restore the libraries represented by the given payload, it is possible to give a restore policy to',
    '    control how to handle existing libraries (default APPEND):',
    '    * FLUSH: delete all existing libraries.',
    '    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.',
    '    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old',
    '      libraries with the new libraries (notice that even on this option there is a chance of failure',
    '      in case of functions name collision with another library).',
  ]
}

export function clientHelpLines(profile: CompatibilityProfile): string[] {
  const redis7 = profile.has('help.client-redis7-text')
  const valkey81 = profile.has('help.client-valkey81-filters')
  const valkey90 = profile.has('help.client-valkey90-filters')
  const primary = profile.has('client.kill.type-primary') ? 'PRIMARY' : 'MASTER'
  const types = `(NORMAL|${primary}|REPLICA|PUBSUB)`

  const lines = [
    HEADER('CLIENT'),
    'CACHING (YES|NO)',
    '    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.',
  ]
  if (profile.has('help.client-capa')) {
    lines.push(
      'CAPA <option> [options...]',
      '    The client claims its some capability options. Options are:',
      '    * REDIRECT',
      '      The client can handle redirection during primary and replica failover in standalone mode.',
    )
  }
  lines.push(
    'GETREDIR',
    '    Return the client ID we are redirecting to when tracking is enabled.',
    'GETNAME',
    '    Return the name of the current connection.',
    'ID',
    '    Return the ID of the current connection.',
    'INFO',
    '    Return information about the current client connection.',
    'KILL <ip:port>',
    '    Kill connection made from <ip:port>.',
    'KILL <option> <value> [<option> <value> [...]]',
    '    Kill connections. Options are:',
    '    * ADDR (<ip:port>|<unixsocket>:0)',
    valkey90
      ? '      Kill connections made from the specified address.'
      : '      Kill connections made from the specified address',
    '    * LADDR (<ip:port>|<unixsocket>:0)',
    valkey90
      ? '      Kill connections made to the specified local address.'
      : '      Kill connections made to specified local address',
    redis7
      ? `    * TYPE ${types}`
      : '    * TYPE (normal|master|replica|pubsub)',
    '      Kill connections by type.',
    '    * USER <username>',
    '      Kill connections authenticated by <username>.',
    '    * SKIPME (YES|NO)',
    '      Skip killing current connection (default: yes).',
  )
  if (profile.has('client.kill.maxage')) {
    lines.push(
      valkey81
        ? '    * ID <client-id> [<client-id>...]'
        : '    * ID <client-id>',
      valkey90
        ? '      Kill connections by client IDs.'
        : valkey81
          ? '      Kill connections by client ids.'
          : '      Kill connections by client id.',
      '    * MAXAGE <maxage>',
      '      Kill connections older than the specified age.',
    )
  }
  if (valkey90) {
    lines.push(
      '    * FLAGS <flags>',
      '      Kill connections that include the specified flags.',
      '    * NAME <client-name>',
      '      Kill connections with the specified name.',
      '    * IDLE <idle>',
      '      Kill connections with idle time greater than or equal to <idle> seconds.',
      '    * LIB-NAME <library-name>',
      '      Kill connections with the specified library name.',
      '    * LIB-VER <library-version>',
      '      Kill connections with the specified library version.',
      '    * DB <db-id>',
      '      Kill connections currently operating on the specified database ID.',
      '    * CAPA <capa>',
      '      Kill connections currently with the specified capa.',
      '    * IP <ip>',
      '      Kill connections made from the specified ip.',
    )
  }
  lines.push(
    'LIST [options ...]',
    '    Return information about client connections. Options:',
    `    * TYPE ${types}`,
    '      Return clients of specified type.',
  )
  if (valkey81) {
    lines.push(
      '    * USER <username>',
      '      Return clients authenticated by <username>.',
      '    * ADDR <ip:port>',
      '      Return clients connected from the specified address.',
      '    * LADDR <ip:port>',
      '      Return clients connected to the specified local address.',
      '    * ID <client-id> [<client-id>...]',
      '      Return clients with the specified IDs.',
      '    * SKIPME (YES|NO)',
      '      Exclude the current client from the list (default: no).',
      '    * MAXAGE <maxage>',
      '      List connections older than the specified age.',
    )
  }
  if (valkey90) {
    lines.push(
      '    * FLAGS <flags>',
      '      Return clients with the specified flags.',
      '    * NAME <client-name>',
      '      Return clients with the specified name.',
      '    * IDLE <idle>',
      '      Return clients with idle time greater than or equal to <idle> seconds.',
      '    * LIB-NAME <lib-name>',
      '      Return clients with the specified lib name.',
      '    * LIB-VER <lib-version>',
      '      Return clients with the specified lib version.',
      '    * DB <db-id>',
      '      Return clients currently operating on the specified database ID.',
      '    * CAPA <capa>',
      '      Return connections currently with the specified capa.',
      '    * IP <ip>',
      '      Return connections made from the specified ip.',
    )
  }
  lines.push(
    'UNPAUSE',
    '    Stop the current client pause, resuming traffic.',
    'PAUSE <timeout> [WRITE|ALL]',
    redis7
      ? '    Suspend all, or just write, clients for <timeout> milliseconds.'
      : '    Suspend all, or just write, clients for <timout> milliseconds.',
    'REPLY (ON|OFF|SKIP)',
    '    Control the replies sent to the current connection.',
    'SETNAME <name>',
    '    Assign the name <name> to the current connection.',
  )
  if (profile.has('client.setinfo')) {
    lines.push(
      'SETINFO <option> <value>',
      '    Set client meta attr. Options are:',
      '    * LIB-NAME: the client lib name.',
      '    * LIB-VER: the client lib version.',
    )
  }
  lines.push(
    'UNBLOCK <clientid> [TIMEOUT|ERROR]',
    '    Unblock the specified blocked client.',
    'TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]',
    redis7 ? '         [OPTIN] [OPTOUT] [NOLOOP]' : '         [OPTIN] [OPTOUT]',
    '    Control server assisted client side caching.',
    'TRACKINGINFO',
    '    Report tracking status for the current connection.',
  )
  if (profile.has('client.no-evict')) {
    lines.push(
      'NO-EVICT (ON|OFF)',
      '    Protect current client connection from eviction.',
    )
  }
  // NO-TOUCH arrived with SETINFO, in 7.2.
  if (profile.has('client.setinfo')) {
    lines.push(
      'NO-TOUCH (ON|OFF)',
      '    Will not touch LRU/LFU stats when this mode is on.',
    )
  }
  if (valkey81) {
    lines.push(
      'IMPORT-SOURCE (ON|OFF)',
      '    Mark this connection as an import source if import-mode is enabled.',
      "    Sync tools can set their connections into 'import-source' state to visit",
      '    expired keys.',
    )
  }
  return lines
}
