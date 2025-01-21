(ns fancy-caching-example.stale-while-refresh
  (:refer-clojure :rename {get cget
                           set cset})
  (:require [clojure.java.io :as io])
  (:import (redis.clients.jedis Jedis
                                JedisPool
                                JedisPubSub
                                HostAndPort
                                DefaultJedisClientConfig)
           (redis.clients.jedis.commands FunctionCommands)
           (java.util.concurrent ExecutorService
                                 Executors
                                 Future
                                 CompletableFuture)))


(set! *warn-on-reflection* true)


(defn- load-dcache-lib! [^Jedis c]
  (with-open [in (-> (io/resource "fancy_caching_example/dcache.lua")
                     (io/input-stream))]
    (let [dcache-lib (.readAllBytes in)]
      (.functionLoadReplace c dcache-lib))))


(defn init [{:keys [entry-factory pool executor encode decode]}]
  (assert (fn? entry-factory) "entry-factory is required")
  (assert (instance? JedisPool pool) "pool is required")
  (assert (instance? ExecutorService executor) "executor is required")
  (assert (or (nil? encode) (fn? encode)) "encode is must be a fn")
  (assert (or (nil? decode) (fn? decode)) "decode is must be a fn")
  (with-open [c (.getResource ^JedisPool pool)]
    (load-dcache-lib! c))
  {:entry-factory entry-factory
   :encode        encode
   :decode        decode
   :pool          pool
   :executor      executor
   :client-id     (str (java.util.UUID/randomUUID))})


(defn halt [cache]
  ; Nothing to cleanup
  )


(defn- get-client ^Jedis [cache]
  (let [^JedisPool pool (-> cache :pool)]
    (.getResource pool)))


(defn- dcache-get [cache key]
  (let [decode (-> cache :decode)]
    (with-open [client (get-client cache)]
      (let [resp (.fcall ^FunctionCommands client
                         "dcache_get"
                         [(str key)]
                         [(:client-id cache)])]
        (if (and (contains? resp :value) decode)
          (update resp :value decode)
          resp)))))


(defn- dcache-set [cache key {:keys [value stale expire]}]
  (let [encode (-> cache :encode)]
    (with-open [client (get-client cache)]
      (.fcall ^FunctionCommands client
              "dcache_set"
              [(str key)]
              [(:client-id cache)
               (if encode (encode value) value)
               (str stale)
               (str expire)]))))


(defn- make-entry ^Future [cache key]
  (let [executor      (-> cache :executor)
        entry-factory (-> cache :entry-factory)
        task          (fn []
                        (let [entry (entry-factory key)]
                          (dcache-set cache key entry)
                          entry))]
    (.submit ^ExecutorService executor ^Callable task)))


(defn- async-get [cache key]
  (println "get:" key)
  (loop [wait-ms 2]
    (let [{:strs [status value]} (dcache-get cache key)]
      (case status
        "OK"      value
        "STALE"   (do (make-entry cache key)
                      value)
        "MISS"    (->> (make-entry cache key)
                       (.get)
                       :value)
        "PENDING" (do (Thread/sleep wait-ms)
                      (recur (min (* wait-ms 2) 1000)))))))


(defn get ^CompletableFuture [cache key]
  (let [^ExecutorService executor (-> cache :executor)]
    (CompletableFuture/supplyAsync (fn [] (async-get cache key)) executor)))


(comment
  (def pool (JedisPool. (HostAndPort. "127.0.0.1" 6379)
                        (-> (DefaultJedisClientConfig/builder)
                            (.clientName "fancy-caching-example")
                            (.resp3)
                            (.build))))
  (def executor (Executors/newVirtualThreadPerTaskExecutor))

  (def cache (init {:entry-factory (fn [key]
                                     (println "entry-factory:" key)
                                     (let [now (System/currentTimeMillis)]
                                       {:value  (str "value for " key)
                                        :stale  (+ now 1000)
                                        :expire (+ now 2000)}))
                    :pool          pool
                    :executor      executor}))

  (let [now (System/currentTimeMillis)]
    (dcache-set cache "foo" {:value  "fofo"
                             :stale  (+ now 10000)
                             :expire (+ now 20000)}))

  (dcache-get cache "foo")

  (with-open [c (.getResource pool)]
    (.eval c "redis.setresp(3); local resp = redis.call(\"HGETALL\", KEYS[1]); redis.log(redis.LOG_WARNING, cjson.encode(resp)); return resp" ["foo"] []))

  (with-open [c (.getResource pool)]
    (.del c "foo"))

  @(get cache "foo")
  ;
  )
