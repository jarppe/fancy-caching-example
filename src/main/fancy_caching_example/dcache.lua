#!lua name=dcache

local miss_timeout = "1000"
local stale_timeout = "1000"

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

  Returns a redis hash with following fields:

    `status`:     The entry status
    `value`:      The cache entry value, if any

  The response `status` can have following values:

  "OK"       The `value` contains a valid response.
  "STALE"    The `value` contains a valid response, but the caller must perform cache value
             refresh using `dcache_set` function. Caller can use the value immediately and
             perform the refresh asynchronously.
  "MISS"     Cache entry is missing (or expired), caller must proceed to generate a value
             and send it to cache using the `dcache_set` function.
  "PENDING"  Cache entry is missing (or expired), but another caller is already working
             to produce a value for cache. Caller should retry after a short timeout.
             Optionally, caller may subscribe to channel `"cache:update"` to get
             notification for cache entry updates (see below).

  Implementation:
  ---------------

  This function uses the Redis HGEALL to get the requested cache entry from Redis.

  If the cache entry is found and the entry indicates that it is not stale, function returns
  with status `"OK"`.

  If the cache entry exists, is stale, and has no leader set, marks the current caller as the
  leader for this cache entry and returns with status `"STALE"`. It is expected that the caller
  proceeds to generate an updated value for the cache entry. Once the updated value is ready
  caller must call `dcache_set` function.

  If the cache entry exists, is stale, and has a leader set, returns with status `"OK"`.

  If the entry is not found and has no leader, marks the current caller as the leader for this
  cache entry and returns the entry as redis with status `"MISS"`. It is expected that the caller
  proceeds to generate a value for cache entry. Once the updated value is ready caller must call
  the `dcache_set` function.

  If the entry is not found, and has a leader, returns with redis hash with status `"PENDING"`.
  This indicates that another caller is already working on to produce a value to cache entry,
  and that this caller should retry the call after a short timeout. Optionally, the client
  can also subscribe to channel with the same name as the cache entry key to get notifications
  when the value is available. It should be noted that the caller should not rely only to the
  pub/sub message. Instead the caller should have some timeout value after it retries the call
  even when the pub/sub message was lost.
]]

redis.register_function("dcache_get", function(keys, args)
  local key = keys[1]
  local client_id = args[1]

  redis.setresp(3)

  local entry = redis.call("HGETALL", key)["map"]
  local value = entry["value"]
  local leader = entry["leader"]

  -- CASE 1: Cache entry has no value and no leader:
  --
  --   Save the client as a leader. Set the whole HASH to expire in miss_timeout so
  --   that if the leader fails the whole HASH is expunged.
  --
  --   Return "MISS" so that client knows it is expected to produce a value
  --   using 'redis.dcache_set'.

  if value == nil and leader == nil then
    redis.call("HSET", key, "leader", client_id) -- Add hash with just the leader
    redis.call("PEXPIRE", key, miss_timeout)     -- Set the hash to expire at miss-timeout
    return {
      map = {
        status = "MISS"
      }
    }
  end

  -- CASE 2: Cache entry has no value, but leader has been elected:
  --
  --   Leader is assigned to produce a value.
  --
  --   Return "PENDING" so that the client knows that it is expected to
  --   wait for a while and try again.

  if value == nil and leader ~= nil then
    return {
      map = {
        status = "PENDING"
      }
    }
  end

  -- CASE 3: Cache hit, but value is getting stale, and there's no leader:
  --
  --   We found valid value, but it should be refreshed.
  --
  --   Assign the client as leader and set the leader entry expiration to
  --   stale_timeout, so that if the client fails the leader entry is expunged
  --   and we get to elect a new leader.
  --
  --   Return "STALE" so that the client known it can use the returned value
  --   immediatelly, but that it should also refresh the value and set it
  --   using 'redis.dcache_set'.

  local stale = entry["fresh"] == nil -- Entry is stale if the "fresh" key has expired

  if stale and leader == nil then
    redis.call("HSET", key, "leader", client_id)                        -- Set the leader
    redis.call("HPEXPIRE", key, stale_timeout, "FIELDS", "1", "leader") -- Set leader to expire ar stale_timeout
    return {
      map = {
        status = "STALE",
        value = value
      }
    }
  end

  -- CASE 4: Cache hit, we have value and it's either not stale, or
  --         it is stale but has a leader elected.
  --
  --   Return "OK".

  return {
    map = {
      status = "OK",
      value = value
    }
  }
end
)

--[[
  Usage:
    | keys  | arguments                             |
    | ----- | ------------------------------------- |
    | <key> | <client-id> <value> <stale> <expire>  |

  Where:
    <key>         The cache entry key. String that uniquely identifies the
                  cache entry.
    <client-id>   Unique ID identifying the client instance. Must be an
                  unique string identifying the client. Clients must generate
                  this key on start-up.
    <value>       New value for cache entry.
    <stale>       Timestamp when cached value becomes stale. After the value
                  becomes stale it can still be used, but a background refresh is
                  started. Expressed as relative milliseconds from current time.
    <expire>      Timestamp when cached value is expired. After the expiration
                  the cached value is not available any more, and a new value
                  must be generated if entry is requested. Expressed as
                  relative milliseconds from current time.

  If the caller called `dcache_get` and the response status was either `"STALE"` or `"MISS"`,
  then the caller is expected to produce an updated value for cache entry. Once the value is
  produced caller must call this function to update the cache entry.

  The return value is either "OK" or "CONFLICT".

  If the return value is "OK", the value was successfully set and the client can proceed to
  use the value normally. This includes saving it to possible local cache etc.

  If the return value is "CONFLICT", the value was not set due to a conflict. Client must
  restart the process of getting the valye from cache using the 'redis.dcache_get'.

  Note that currently the return value is always "OK" and the "CONFLICT" reply is not used,
  but clients should be prepared to handle "CONFLICT" anyway to be compatible with possible
  future versions.
]]

redis.register_function("dcache_set", function(keys, args)
  local key    = keys[1]
  -- local client_id = args[1]  ; This is un-used now, but good to have for possible future use
  local value  = args[2]
  local stale  = args[3]
  local expire = args[4]

  redis.call("HSET", key,
    "value", value,                                          -- Save the value
    "fresh", "t")                                            -- Add flag to indicate freshness
  redis.call("HDEL", key, "leader")                          -- Remove leader
  redis.call("HPEXPIRE", key, stale, "FIELDS", "1", "fresh") -- Set the "fresh" to expire at stale time
  redis.call("PEXPIRE", key, expire)                         -- Set the entry to expire at expire time
  redis.call("PUBLISH", "dcache_set:set", key)               -- Publish cache update

  return "OK"
end
)
