(ns jarppe.cache.dcache
  (:require [clojure.java.io :as io])
  (:import (redis.clients.jedis Jedis)
           (redis.clients.jedis.commands FunctionCommands)))


(set! *warn-on-reflection* true)


(defn load-dcache-lib [client]
  (with-open [in (-> (io/resource "jarppe/cache/dcache.lua")
                     (io/input-stream))]
    (.functionLoadReplace ^Jedis client (.readAllBytes in))))


(defn fcall ^java.util.Map [client fname key args]
  (.fcall ^FunctionCommands client fname [key] args))

