# Fancy caching example

Experimenting with Redis caching that avoids the [stampede problem](https://en.wikipedia.org/wiki/Cache_stampede), does automatic background cache refresh, and helps tom maintain data consistency with clients that do local in-memmory caching on top of Redis caching.

## What is this?

This is my experimentation with Clojure implementation for caching with Redis.

This has three goals. First, it tries to avoif the cache stampede problem. When multiple clients make simultaneus queries to the cache for a key that is not in cache, typically the cache responds to each query with cache MISS. This causes all clients to build a value (like making queries to DB to look for values to be cached). Eventually all clients push the value to cache. This can cause unnecessary load to system and can cause data inconsitencies.

Second goal is to introduce a background cache refresh. When cache detects that the cached value is close to become expured, it can trigger a background process to update the cache before the value is expired. This way the cache can provide fresh value from cache consistently.

Finally, the implementation helps to keep possible client in-memory caches in sync. In the case of cache stampede, it is possible that some of the clients produce a more recent value than others. If the clients save the value they push to Redis in their own in-memory caches for faster serving, they end up serving different values.

## Implementation

This implementation tries to meet these goals by instructing only the first client that it should build the value for cache, and instruct all other clients to wait and retry. Once the first client has constructed the value and submitted it to cache, the other clients can fetch the cached value. If the first client fails to submit value (maybe it crashed), a second client is selected to perform the value construction.

The implementation has a book keepping where it maintains information about `"leader"`; the client that is currently selected to produce a value for missing or stale entry. The implementation utilizes Redis expiration mechanism to clear the leader infor after a certain timeout. This is used to detect a client that has failed to produce a value.

Part of the code is in Lua source that is loaded into Redis as a [Redis functions](https://redis.io/docs/latest/develop/interact/programmability/functions-intro/). Note that this requires a Redis version 7.

## How?

This implementation implements a [Lua library](./src/main/fancy_caching_example/dcache.lua) with two functions, one for reading cache entries and one for storing cache entries. In addition this implementation has a Clojure namespace with function to make cache requests. All the logic with cache updates and waits is incorporated to these two parts. Users of this Clojure library see only one simple [get](./src/main/fancy_caching_example/stale_while_refresh.clj#109) function.

## Is that it?

Actually, no. This implementation has an additional fancy feature: "stale while refresh" (also known as "stale while revalidate").

When the cache request makes a hit (valid value is found from cache), but the cache detects that the value will became "old" soon (and thus be expired from cache), it starts a background refresh. If the refresh is successful (an updated value is constructed) it will be save to cache for later use. This means that for "hot" cache entries (entries that are requested often) clients get fresh values from cache without waiting. This can improve the system performance.

## So to the production then?

Not quite so fast. This is just an experimentation. There are some open questions that should be decided and also some real-life testing to be done before I can recommed this for general use.

## Considerations for future

- [⎷] Add cache a name and use that name as prefix to allow deploying multiple cache instance in same database
- [⎷] Add possibility to set default stale and expire times for cache instances so that factories do not need to produce them
- [⎷] Consider publishing a cache updated message when dcache_set is called
- [⎷] Consider detecting cache updated messages in client when waiting
- [ ] Improve data consistency by providing means to remove updated values from local caches when cache update message is received
- [⎷] Consider rejecting dcache_set calls from clients that are not currently leaders (see [below](#handling-update-conflicts))
- [⎷] Consider using relative times for stale and expiration times handle possible clock differences between client and redis instance
- [ ] Run performance analysis and do stress testing

### Handling update conflicts

When client is elected as leader, the client ID is saved as a key `"leader"` in cache entry. This value is set to have an expiration time so that if the elected leader fails the `"leader"` value is expunged and the library can elect a new leader.

What happens it the original leader does not crash, but instead it was just delayed. The cache would detect that the leader is expired and elects a new leader. Now we have two clients working to produce a value for cache.

What happends when the originally elected client calls `dcache_set`?

Current implementation acceptes all calls to `dcache_set` regardless of the client ID. This measn that bot clients, the original leader and the newly elected leader both make a successful call to `dcache_set`. Later call overwrites the previous one.

But if the clients also save the value they produced in their own in-memory caches, they now have inconsistent states.

We could change the `dcache_set` to reject the call from client if that client ID is not the currently elected leader. It could instead return a status `"CONFLICT"` which would cause the client to restart the whole process from the start by calling `dcache_get`. This change would ensure that all clients end up having consistent state.

How ever, there is a risk. If the process of producing an updated value for cache starts to consistently take longer than the leader timeout, we end up in a situation where we elect a new leader repeatedly, but non of the leaders are able to procuce a value before the new leader is elected. We end up in infinite loop.

So this is a tradeoff between fault tolerancy (or actually "slowness tolerancy") and data consistency.

## License

Copyright © 2025 Jarppe Länsiö.

Available under the terms of the Eclipse Public License 2.0.
