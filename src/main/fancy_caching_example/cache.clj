(ns fancy-caching-example.cache
  (:refer-clojure :rename {get cget})
  (:require [fancy-caching-example.cache.notification :as notif]
            [fancy-caching-example.cache.dcache :as dcache])
  (:import (java.time Duration)
           (java.util.concurrent Executor
                                 Executors)
           (redis.clients.jedis JedisPool
                                HostAndPort
                                DefaultJedisClientConfig)))


(set! *warn-on-reflection* true)


;; Returns a function that accepts a cache-key, calls Redis function `dcache_get`, and
;; returns a vector with status and cache value:

(defn- make-dcache-get [{:keys [pool cache-name client-id decode]}]
  (fn [cache-key]
    (let [^java.util.Map resp   (dcache/fcall pool
                                              "dcache_get"
                                              (str cache-name ":" cache-key)
                                              [client-id])
          status (.get resp "status")
          value  (.get resp "value")]
      [status (when value (decode value))])))


;; Returns a function that accepts cache-key, new value, a stale and expire times. The
;; returned function calls the Redis `dcache_set` function with these values and returns
;; the status:

(defn- make-dcache-set [{:keys [pool cache-name client-id encode]}]
  (fn [cache-key value stale expire]
    (dcache/fcall pool
                  "dcache_set"
                  (str cache-name ":" cache-key)
                  [client-id
                   (encode value)
                   (str stale)
                   (str expire)])))


;; Returns a function that accepts a cache-key for new cache entry. The returned function
;; then invokes the entiry-factory to create new value for cache key, and then calls
;; the `dcache-set` function to set the valye on Redis. If the value is successfully
;; set in Redis, returns the new value. If the Redis rejected the value, returns `nil`:


(defn- make-new-entry [{:keys [entry-factory default-stale-ms default-expire-ms]} dcache-set]
  (fn [cache-key]
    (let [value  (entry-factory cache-key)
          stale  (or (-> value (meta) :stale) default-stale-ms -1)
          expire (or (-> value (meta) :expire) default-expire-ms)]
      (when (nil? value)  (throw (ex-info "value can not be nil" {})))
      (when (nil? expire) (throw (ex-info "expiration time missing" {})))
      (when (= (dcache-set cache-key value stale expire)
               "OK")
        value))))


;; Returns a function that accepts cache-key, timeout, and timeout-value. The returned
;; function returns the value from cache. If the timeout is reached before the value
;; is available returns timeout-value. If timeout is `nil` then retries until thread 
;; is interrupted:

(defn- make-get-cache-value [{:keys [init-wait-ms max-wait-ms ^Executor executor listener]
                              :as   config}]
  (let [dcache-get (make-dcache-get config)
        dache-set  (make-dcache-set config)
        new-entry  (make-new-entry config dache-set)]
    (fn [cache-key timeout timeout-value]
      (let [deadline (if timeout
                       (-> (Duration/ofMillis timeout)
                           (.toNanos)
                           (+ (System/nanoTime)))
                       Long/MAX_VALUE)]
        (loop [wait-ms init-wait-ms]
          (if (> (System/nanoTime) deadline)
            timeout-value
            (let [[status value] (dcache-get cache-key)]
              (case status
                          ;; Cache hit:
                "OK"      value
                          ;; Cache miss, create cache entry and try to save it to cache.
                          ;; If successful, return the created entry. If not, try again 
                          ;; starting with the initial wait time:
                "MISS"    (if-let [entry (new-entry cache-key)]
                            entry
                            (recur init-wait-ms))
                          ;; Cached hit, but entry it's getting stale. Return the cached value but
                          ;; start a refresh in background:
                "STALE"   (do (.execute executor (fn [] (new-entry cache-key)))
                              value)
                          ;; Cache miss, but some other client is already elected as a leader
                          ;; and is making the entry. Wait for a while and try again:
                "PENDING" (do (notif/wait-notification listener cache-key wait-ms)
                              (recur (min (* wait-ms 2)
                                          max-wait-ms)))))))))))


;;
;; Public API:
;; ===========
;;


(deftype Cache [cache-name client-id get-cache-value on-close]
  clojure.lang.ILookup
  (valAt [_this cache-key]   (get-cache-value cache-key nil nil))
  (valAt [_this cache-key _] (get-cache-value cache-key nil nil))

  clojure.lang.IFn
  (invoke [_this cache-key]                       (get-cache-value cache-key nil     nil))
  (invoke [_this cache-key timeout]               (get-cache-value cache-key timeout nil))
  (invoke [_this cache-key timeout timeout-value] (get-cache-value cache-key timeout timeout-value))
  (applyTo [_this seq]
    (let [[cache-key timeout timeout-value] seq]
      (get-cache-value cache-key timeout timeout-value)))

  java.io.Closeable
  (close [_] (on-close))

  clojure.lang.Named
  (getName [_] cache-name)

  java.lang.Object
  (toString [_] (format "Cache[cache-name=\"%s\",client-id=\"%s\"]" cache-name client-id))
  (equals [this that] (identical? this that)))


(defn init [{:keys [cache-name
                    entry-factory
                    pool
                    executor
                    encode
                    decode
                    default-stale-ms
                    default-expire-ms
                    init-wait-ms
                    max-wait-ms
                    client-id]
             :or   {executor     (Executors/newVirtualThreadPerTaskExecutor)
                    encode       str
                    decode       identity
                    init-wait-ms 2
                    max-wait-ms  200
                    client-id    (str (java.util.UUID/randomUUID))}}]
  (assert cache-name                 "cache-name is required")
  (assert (name cache-name)          "cache-name is named")
  (assert (fn? entry-factory)        "entry-factory is required")
  (assert (instance? JedisPool pool) "Jedis pool is required")
  (dcache/load-dcache-lib pool)
  (let [cache-name      (name cache-name)
        listener        (notif/notification-listener {:cache-name cache-name
                                                      :executor   executor
                                                      :pool       pool})
        get-cache-value (make-get-cache-value {:pool              pool
                                               :cache-name        cache-name
                                               :client-id         client-id
                                               :decode            decode
                                               :encode            encode
                                               :entry-factory     entry-factory
                                               :default-stale-ms  default-stale-ms
                                               :default-expire-ms default-expire-ms
                                               :init-wait-ms      init-wait-ms
                                               :max-wait-ms       max-wait-ms
                                               :executor          executor
                                               :listener          listener})
        on-close        (fn [] (notif/close listener))]
    (Cache. cache-name
            client-id
            get-cache-value
            on-close)))


(defn close [^Cache cache]
  (when cache
    (.close cache)))


;; Experimenting:


(comment

  (def pool (JedisPool. (HostAndPort. "redis" 6379)
                        (-> (DefaultJedisClientConfig/builder)
                            (.clientName "fancy-caching-example")
                            (.resp3)
                            (.build))))

  (def cache (init {:cache-name    "hello"
                    :pool          pool
                    :entry-factory (fn [key]
                                     {:value  (str "value for " key)
                                      :stale  1000
                                      :expire 2000})}))

  (cache "foo")
  ;;=> "value for foo"

  (.close cache)
  (.close pool)
  ;
  )
