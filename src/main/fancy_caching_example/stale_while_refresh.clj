(ns fancy-caching-example.stale-while-refresh
  (:refer-clojure :rename {get cget})
  (:require [clojure.java.io :as io])
  (:import (java.util.function Supplier)
           (java.util.concurrent Executor
                                 Executors
                                 ExecutorService 
                                 Future
                                 CompletableFuture)
           (redis.clients.jedis JedisPool
                                JedisPubSub
                                HostAndPort
                                DefaultJedisClientConfig)
           (redis.clients.jedis.commands FunctionCommands)))


(set! *warn-on-reflection* true)


;;
;; Helpers:
;;


(defn- supply-async ^CompletableFuture [^Executor executor ^Supplier task]
  (CompletableFuture/supplyAsync task executor))


(def ^:private ^String/1 dcache-set-channel-name (into-array String ["dcache_set:set"]))


;;
;; Listeners support:
;;


(defrecord Listener [key promise]
  java.lang.Object
  (toString [_] (str "Listener:[" (pr-str key) "]")))


(defn- notify-listeners [listeners key]
  (doseq [^Listener listener @listeners]
    (when (= (.key listener) key)
      (deliver (.promise listener) listener))))


(defn- wait-for [listeners key timeout]
  (let [p (promise)]
    (vswap! listeners conj (Listener. key p))
    (deref p timeout ::ignored)
    (vswap! listeners (partial remove (fn [^Listener listener] (identical? (.promise listener) p))))))


;;
;; Cache life-cycle:
;; =================
;;


(defrecord Cache [cache-prefix entry-factory pool executor encode decode client-id listeners on-close init-wait-ms max-wait-ms factory-defaults]
  java.io.Closeable
  (close [_]
    (on-close)))


(defn init [config]
  (let [{:keys [cache-name entry-factory pool executor encode decode]} config]
    (assert (fn? entry-factory) "entry-factory is required")
    (assert (instance? JedisPool pool) "pool is required")
    (assert (instance? ExecutorService executor) "executor is required")
    (assert (or (nil? encode) (fn? encode)) "encode is must be a fn")
    (assert (or (nil? decode) (fn? decode)) "decode is must be a fn") 
    (let [client            (.getResource ^JedisPool pool)
          _                 (with-open [in (-> (io/resource "fancy_caching_example/dcache.lua")
                                               (io/input-stream))]
                              (.functionLoadReplace client (.readAllBytes in)))
          listeners         (volatile! ())
          cache-prefix      (str (or cache-name "cache") ":")
          cache-prefix-len  (count cache-prefix)
          pubsub            (proxy [JedisPubSub] []
                              (onMessage [_ message]
                                (let [key (subs message cache-prefix-len)]
                                  (notify-listeners listeners key))))
          fut       (supply-async executor (fn [] (.subscribe client pubsub dcache-set-channel-name)))]
      (map->Cache {:cache-prefix     cache-prefix
                   :entry-factory    entry-factory
                   :pool             pool
                   :executor         executor
                   :encode           encode
                   :decode           decode
                   :client-id        (str (java.util.UUID/randomUUID))
                   :listeners        listeners
                   :init-wait-ms     (:init-wait-ms config 2)
                   :max-wait-ms      (:max-wait-ms config 1000)
                   :factory-defaults {:stale  (:default-stale config)
                                      :expire (:default-expire config)}
                   :on-close         (fn []
                                       (.unsubscribe pubsub)
                                       (.get fut)
                                       (.close client))}))))


(defn halt [^Cache cache]
  (when cache
    (.close cache)))


;;
;; Private stuff:
;; ==============
;;


;;
;; Wrappers for calling dcache Redis functions:
;;

(defn- fcall [^JedisPool pool fname key args]
  (with-open [client (.getResource pool)]
    (.fcall ^FunctionCommands client fname [key] args)))


(defn- dcache-get [^Cache cache key]
  (let [resp   (fcall (.pool cache) "dcache_get" (str (.cache-prefix cache) key) [(.client-id cache)])
        decode (.decode cache)]
    (if (and (contains? resp :value) decode)
      (update resp :value decode)
      resp)))


(defn- dcache-set [^Cache cache key {:keys [value stale expire]}]
  (fcall (.pool cache)
         "dcache_set"
         (str (.cache-prefix cache) key)
         [(.client-id cache)
          (if-let [encode (.encode cache)]
            (encode value)
            value)
          (str stale)
          (str expire)]))

;;
;; Invoking cache value factory:
;;


(defn- make-entry ^Future [^Cache cache key]
  (supply-async (.executor cache)
                (fn []
                  (let [entry-factory (.entry-factory cache)
                        entry         (merge (.factory-defaults cache)
                                             (entry-factory key))]
                    (when (= (dcache-set cache key entry) "OK")
                      entry)))))


;;
;; dcache_get loop:
;;


(defn- do-get [^Cache cache key]
  (loop [wait-ms (.init-wait-ms cache)]
    (let [{:strs [status value]} (dcache-get cache key)] 
      (case status
        "OK"      value
        "STALE"   (do (make-entry cache key)
                      value)
        "MISS"    (-> (make-entry cache key)
                      (.get)
                      :value
                      (or (recur (.init-wait-ms cache))))
        "PENDING" (do (wait-for (.listeners cache) key wait-ms)
                      (recur (min (* wait-ms 2) 
                                  (.max-wait-ms cache))))))))


;;
;; Public API:
;; ===========
;;


(defn get ^CompletableFuture [^Cache cache key]
  (supply-async (.executor cache)
                (fn []
                  (do-get cache key))))


;;
;; Experimenting:
;;


(comment

  (def pool (JedisPool. (HostAndPort. "redis" 6379)
                        (-> (DefaultJedisClientConfig/builder)
                            (.clientName "fancy-caching-example")
                            (.resp3)
                            (.build))))
  (def executor (Executors/newVirtualThreadPerTaskExecutor))

  (def cache (init {:entry-factory (fn [key]
                                     (println "entry-factory:" key)
                                     {:value  (str "value for " key)
                                      :stale  1000
                                      :expire 2000})
                    :pool          pool
                    :executor      executor}))

  (with-open [c (.getResource pool)]
    (.del c "foo"))
  ;;=> 0

  @(get cache "foo")
  ;;=> "value for foo"

  (dcache-set cache "fofo" {:value  "value"
                            :stale  "1000"
                            :expire "2000"})
  ;
  )

