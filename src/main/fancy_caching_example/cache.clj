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


;;
;; Cache life-cycle:
;; =================
;;


(declare get)


(deftype Cache [cache-name entry-factory pool executor encode decode client-id listeners on-close init-wait-ms max-wait-ms default-stale default-expire]
  clojure.lang.ILookup
  (valAt [this k] (get this k))
  (valAt [this k _] (get this k))

  clojure.lang.IFn
  (invoke [this k] (get this k))
  (invoke [this k _] (get this k))

  clojure.lang.Associative
  (entryAt [this k] (clojure.lang.MapEntry. k (get this k)))

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
  (assert (string? cache-name)       "cache-name is required")
  (assert (fn? entry-factory)        "entry-factory is required")
  (assert (instance? JedisPool pool) "Jedis pool is required")
  (let [listeners    (notif/notification-listeners)
        subscription (notif/notification-subscription cache-name executor pool listeners)
        on-close     (fn [] (.close subscription))]
    (dcache/load-dcache-lib pool)
    (Cache. cache-name
            entry-factory
            pool
            executor
            encode
            decode
            client-id
            listeners
            on-close
            init-wait-ms
            max-wait-ms
            default-stale-ms
            default-expire-ms)))


(defn close [^Cache cache]
  (when cache
    (.close cache)))


;;
;; Invoking cache value factory:
;;


(defn dcache-get [^Cache cache key]
  (let [^java.util.Map resp   (dcache/fcall (.pool cache)
                                            "dcache_get"
                                            (str (.cache-name cache) ":" key)
                                            [(.client-id cache)])
        status (.get resp "status")
        value  (.get resp "value")
        decode (.decode cache)]
    [status (when value (decode value))]))


(defn dcache-set [^Cache cache key value stale expire]
  (dcache/fcall (.pool cache)
                "dcache_set"
                (str (.cache-name cache) ":" key)
                [(.client-id cache)
                 ((.encode cache) value)
                 (str stale)
                 (str expire)]))


(defn- make-and-set-entry [^Cache cache key]
  (let [entry-factory (.entry-factory cache)
        entry         (entry-factory key)
        value         (or (:value entry)
                          entry)
        stale         (or (:stale entry)
                          (.default-stale cache)
                          -1)
        expire        (or (:expire entry)
                          (.default-expire cache))]
    (when (nil? value)  (throw (ex-info "value can not be nil" {})))
    (when (nil? expire) (throw (ex-info "expiration time missing" {})))
    (when (= (dcache-set cache key value stale expire) "OK")
      value)))


;;
;; Public API:
;; ===========
;;


(defn get
  ([^Cache cache cache-key] (get cache cache-key nil nil))
  ([^Cache cache cache-key timeout] (get cache cache-key timeout nil))
  ([^Cache cache cache-key timeout timeout-value]
   (let [deadline (if timeout
                    (-> (Duration/ofMillis timeout)
                        (.toNanos)
                        (+ (System/nanoTime)))
                    Long/MAX_VALUE)]
     (loop [wait-ms (.init-wait-ms cache)]
       (if (> (System/nanoTime) deadline)
         timeout-value
         (let [[status value] (dcache-get cache cache-key)]
           (case status
             ;; Cache hit:
             "OK"      value
             ;; Cache miss, create cache entry and try to save it to cache.
             ;; If successful, return the created entry. If not, try again 
             ;; starting with the initial wait time:
             "MISS"    (if-let [entry (make-and-set-entry cache cache-key)]
                         entry
                         (recur (.init-wait-ms cache)))
             ;; Cached hit, but entry it's getting stale. Return the cached value but
             ;; start a refresh in background:
             "STALE"   (let [^Executor executor (.executor cache)]
                         (.execute executor (fn [] (make-and-set-entry cache cache-key)))
                         value)
             ;; Cache miss, but some other client is already elected as a leader
             ;; and is making the entry. Wait for a while and try again:
             "PENDING" (do (notif/wait-notification (.listeners cache) cache-key wait-ms)
                           (recur (min (* wait-ms 2)
                                       (.max-wait-ms cache)))))))))))


;;
;; Experimenting:
;;


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

  (get cache "foo")
  ;;=> "value for foo"
  
  (.close cache)
  (.close pool)
  )
