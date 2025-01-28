(ns fancy-caching-example.stale-while-refresh
  (:refer-clojure :rename {get cget})
  (:require [clojure.java.io :as io])
  (:import (java.util.concurrent ExecutorService
                                 Executors
                                 Future
                                 CompletableFuture)
           (redis.clients.jedis JedisPool
                                HostAndPort
                                DefaultJedisClientConfig)
           (redis.clients.jedis.commands FunctionCommands)))


(set! *warn-on-reflection* true)


;;
;; Cache life-cycle:
;; =================
;;


(defn init [{:keys [entry-factory pool executor encode decode]}]
  (assert (fn? entry-factory) "entry-factory is required")
  (assert (instance? JedisPool pool) "pool is required")
  (assert (instance? ExecutorService executor) "executor is required")
  (assert (or (nil? encode) (fn? encode)) "encode is must be a fn")
  (assert (or (nil? decode) (fn? decode)) "decode is must be a fn")
  ;; Load dcache library:
  (with-open [c  (.getResource ^JedisPool pool)
              in (-> (io/resource "fancy_caching_example/dcache.lua")
                     (io/input-stream))]
    (.functionLoadReplace c (.readAllBytes in)))
  ;; Return cache "context"
  {:entry-factory entry-factory
   :encode        encode
   :decode        decode
   :pool          pool
   :executor      executor
   :client-id     (str (java.util.UUID/randomUUID))})


(defn halt [_cache]
  ; Nothing to cleanup
  )


;;
;; Private stuff:
;; ==============
;;


(defn- fcall [{:keys [^JedisPool pool]} fname keys args]
  (with-open [c (.getResource pool)]
    (.fcall ^FunctionCommands c fname keys args)))


(defn- dcache-get [cache key]
  (let [resp   (fcall cache "dcache_get" [(str key)] [(:client-id cache)])
        decode (-> cache :decode)]
    (if (and (contains? resp :value) decode)
      (update resp :value decode)
      resp)))


(defn- dcache-set [cache key {:keys [value stale expire]}]
  (fcall cache
         "dcache_set"
         [(str key)]
         [(:client-id cache)
          (if-let [encode (-> cache :encode)]
            (encode value)
            value)
          (str stale)
          (str expire)]))


(defn- make-entry ^Future [cache key]
  (let [executor      (-> cache :executor)
        entry-factory (-> cache :entry-factory)
        task          (fn []
                        (let [entry (entry-factory key)]
                          (dcache-set cache key entry)
                          entry))]
    (.submit ^ExecutorService executor ^Callable task)))


(defn- do-get [cache key]
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


;;
;; Public API:
;; ===========
;;


(defn get ^CompletableFuture [cache key]
  (let [^ExecutorService executor (-> cache :executor)]
    (CompletableFuture/supplyAsync (fn [] (do-get cache key)) executor)))


;;
;; Experimenting:
;;


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

  (with-open [c (.getResource pool)]
    (.del c "foo"))
  ;;=> 0

  @(get cache "foo")
  ;;=> "value for foo"

  ;
  )

