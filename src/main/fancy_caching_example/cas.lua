--[[
  Call like redis SET command, except that the first arg is the expected
  current value.

  If the current value is the expected value, calls SET with the rest of
  the args. Returns what ever the SET returns. If the expected value does
  not match returns `nil`.
]]

redis.setresp(3)

local key = KEYS[1]
local sentinel = ARGV[1]
local old_value = redis.call('get', key)

if (
      (sentinel == '' and old_value == false)
      or
      (sentinel == old_value)
    ) then
  table.remove(ARGV, 1)
  return redis.call('set', key, unpack(ARGV))
end

return nil
