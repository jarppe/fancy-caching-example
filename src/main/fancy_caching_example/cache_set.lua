--[[
  Usage:
    | keys  | arguments                                 |
    | ----- | ----------------------------------------- |
    | <key> | <client-id> <value> <stale> <expiration>  |

  Where:
    <key>         The cache entry key. String that uniquely identifies the
                  cache entry.
    <client-id>   Unique ID identifying the client instance. Must be an
                  unique string identifying the client. Clients must generate
                  this key on start-up.
    <value>       New value for cache entry.
    <stale>       Timestamp when cached value becomes stale. After the value
                  becomes stale it can still be used, but a background refresh
                  is started. Expressed as milliseconds from Unix epoch.
    <expiration>  Timestamp when cached value is expired. After the expiration
                  the cached value is not available any more, and a new value
                  must be generated. Expressed as milliseconds from Unix epoch.

  If caller calle `cache_get` and the response status has either `"STALE"` or `"MISS"` value,
  then the caller is expected to produce an updated value for cache entry. Once the value is
  produced caller must call this function to update the cache entry.

  The return value is always "OK".
]]

redis.setresp(3)

local key = KEYS[1]
local client_id = ARGV[1]
local value = ARGV[2]
local stale = ARGV[3]
local expiration = ARGV[4]

return {
  client_id = client_id,
  value = value,
  stale = stale,
  expiration = expiration
}
