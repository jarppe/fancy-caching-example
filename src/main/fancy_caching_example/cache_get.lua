--[[
  Usage:
    | keys  | arguments   |
    | ----- | ----------- |
    | <key> | <client-id> |

  Where:
    <key>         The cache entry key. String that uniquely identifies the
                  cache entry.
    <client-id>   Unique ID identifying the client instance. Must be an
                  unique string identifying the client. Clients must generate
                  this key on start-up.

  Returns a redis hash with following items:

    `status`:     The entry status
    `value`:      The cache entry value, if any
    `stale-at`:   The time when entry becomes stale
    `expire-at`:  The time when entry becomes expired
    `leader`:     The current leader working with cache entry, if any

  The response `status` can have following values:

  "OK"       The `value` contains a valid response.
  "STALE"    The `value` contains a valid response, but the caller must perform cache value
             refresh using `cache_set` function. Caller can use the value immediately and
             perform the refresh asynchronously.
  "MISS"     Cache entry is missing (or expired), caller must proceed to generate a value
             and send it to cache using the `cache_set` function.
  "PENDING"  Cache entry is missing (or expired), but another caller is already working
             to produce a value for cache. Caller should retry after a short timeout.
             Optionally, caller may subscribe to channel `"cache:update"` to get
             notification for cache entry updates (see below).

  Implementation:
  ---------------

  This function uses the Redis HGET to get the requested cache entry from Redis.

  If the cache entry is found and the entry indicates that it is not stale, function returns
  with status `"OK"`.

  If the cache entry exists, is stale, and has no leader set, marks the current caller as the
  leader for this cache entry and returns with status `"STALE"`. It is expected that the caller
  proceeds to generate an updated value for the cache entry. Once the updated value is ready
  caller must call `cache_set` function.

  If the cache entry exists, is stale, and has a leader set, returns with status `"OK"`.

  If the entry is not found and has no leader, marks the current caller as the leader for this
  cache entry and returns the entry as redis with status `"MISS"`. It is expected that the caller
  proceeds to generate a value for cache entry. Once the updated value is ready caller must call
  the `cache_set` function.

  If the entry is not found, and has a leader, returns with redis hash with status `"PENDING"`.
  This indicates that another caller is already working on to produce a value to cache entry,
  and that this caller should retry the call after a short timeout. Optionally, the client
  can also subscribe to channel with the same name as the cache entry key to get notifications
  when the value is available. It should be noted that the caller should not rely only to the
  pub/sub message. Instead the caller should have some timeout value after it retries the call
  even when the pub/sub message was lost.
]]

redis.setresp(3)

local key = KEYS[1]
local client_id = ARGV[1]

return {
  map = {
    key = key,
    client_id = client_id
  }
}
