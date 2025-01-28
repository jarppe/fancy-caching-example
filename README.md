# Fancy caching example

Experimenting with Redis caching that avoids the [stampede problem](https://en.wikipedia.org/wiki/Cache_stampede).

## What is this?

This is my experimentation with Clojure implementation for caching with Redis in a way that avoids the cache stampede problem. When multiple clients make simultaneus queries to the cache for a key that is not in cache, typically the cache responds to each query with cache MISS. This causes all clients to build a cache entry (like making queries to DB to look for values to be cached), and eventually all of them make update to cache.

This implementation avoids this problem by instructing only the first client that it should build the value for cache, and all other clients to wait. Once the first client has constructed the value and submitted it to cache, the other clients can fetch the cached value. If the first client fails to submit value (maybe it crashed), a second client is selected to perform the value construction.

This implementation has two benefits.

First, the load to system is reduced because only one client constructs the value for cache. Forexample, if the value comes from database, only one client makes the query to the DB.

Secondly, the data served from cache is more consustent. Without the stampede protection, clients create value for cache and they all could generate slightly different value. If the clients have their own in-memory cache, they could all serve slightly different values. Forexample, if you have multimple instances of the same service running and serving HTTP requests from end-users. With the stampede protection the cached values are more consistent over the time.

## How?

This implementation implements a [Lua library](./src/main/fancy_caching_example/dcache.lua) with two functions, one for reading cache entries and one for storing cache entries. In addition this implementation has a Clojure namespace with function to make cache requests. All the logic with cache updates and waits is incorporated to these two parts. Users of this Clojure library see only one simple [get](./src/main/fancy_caching_example/stale_while_refresh.clj#109) function.

## Is that it?

Actually, no. This implementation has an additional fancy feature: "stale while refresh" (also known as "stale while revalidate").

When the cache request makes a hit (valid value is found from cache), but the cache detects that the value will became "old" soon (and thus be expired from cache), it starts a background refresh. If the refresh is successful (an updated value is constructed) it will be save to cache for later use. This means that for "hot" cache entries (entries that are requested often) clients get fresh values from cache without waiting. This can improve the system performance.

## So to the production then?

Not quite so fast. This is just an experimentation.

## License

Copyright © 2025 Jarppe Länsiö.

Available under the terms of the Eclipse Public License 2.0.
