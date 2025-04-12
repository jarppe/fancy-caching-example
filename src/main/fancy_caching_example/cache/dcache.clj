(ns fancy-caching-example.cache.dcache
  (:require [clojure.java.io :as io])
  (:import (redis.clients.jedis JedisPool)
           (redis.clients.jedis.commands FunctionCommands)))


(set! *warn-on-reflection* true)


(defn load-dcache-lib [^JedisPool pool]
  (with-open [client (.getResource pool)
              in     (-> (io/resource "fancy_caching_example/cache/dcache.lua")
                         (io/input-stream))]
    (.functionLoadReplace client (.readAllBytes in))))


(defn fcall [^JedisPool pool fname key args]
  (with-open [client (.getResource pool)]
    (.fcall ^FunctionCommands client fname [key] args)))

