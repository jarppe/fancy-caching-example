(ns fancy-cache.example
  (:require [fancy-caching-example.stale-while-refresh :as cache])
  (:import (redis.clients.jedis JedisPool
                                HostAndPort
                                DefaultJedisClientConfig)
           (java.util.concurrent Executors)))

;; We need a Redis connection pool and executor. The executor is used for
;; background refresh operations:

(defonce redis-pool (JedisPool. (HostAndPort. "127.0.0.1" 6379)
                                (-> (DefaultJedisClientConfig/builder)
                                    (.clientName "fancy-caching-example")
                                    (.resp3)
                                    (.build))))

(defonce executor (Executors/newVirtualThreadPerTaskExecutor))

;; We need a function that can create a value for cache. This is called in cache
;; misses, and also when we do stale-while-revalidate thing:

(defn cached-value-factory [key]
  (println (format "cached-value-factory: creating value for key [%s]" key))
  ;; Typically this makes query to DB etc. Here we simulate that with 0.5 sec sleep.
  (Thread/sleep 500)
  ;; Return a map with three elements:
  {:value  (str "value for key " key)  ;; The value to be cached
   :stale  1000                        ;; Value becomes stale in 1 sec
   :expire 2000})                      ;; Value expires is 2 sec

;; Create the cache instance, refresh in every reload of this ns:

(def cache nil)
(alter-var-root #'cache (fn [cache]
                          (when cache
                            (cache/halt cache))
                          (cache/init {:entry-factory cached-value-factory
                                       :pool          redis-pool
                                       :executor      executor})))


(comment

  (java.util.Locale/setDefault (java.util.Locale/of "us" "EN"))

  ;; The only api we have is the cache creation `cache/init` and the `cache/get`. Let's examine
  ;; the get function:

  (type (cache/get cache "foo"))
  ;;=> java.util.concurrent.CompletableFuture

  ;; So we can `defer` it:

  (-> (cache/get cache "foo")
      (deref))
  ;;=> "value for key foo"

  ;; By returning a future we can easily compose async flows with `get`.

  ;; Call `get` multiople times, measuring the times:

  (defn call-get-and-print-time []
    (let [start (System/currentTimeMillis)
          value (deref (cache/get cache "foo"))
          end   (System/currentTimeMillis)]
      (println (format "got value [%s] from cache in %.3f sec"
                       value
                       (-> (- end start) (/ 1000.0))))))

  ;; Call get 5 times in a loop:

  (dotimes [n 5]
    (call-get-and-print-time))

  ;=> prints:
  ; cached-value-factory: creating value for key [foo]
  ; got value [value for key foo] from cache in 0.509 sec
  ; got value [value for key foo] from cache in 0.001 sec
  ; got value [value for key foo] from cache in 0.002 sec
  ; got value [value for key foo] from cache in 0.001 sec
  ; got value [value for key foo] from cache in 0.001 sec

  ;; Notice that the first time we got value from cache in 0.509 sec, because the
  ;; factory took 500 ms. Rest of the calls come fast.

  ;; Try to hit cache between stale and expiration to see if we can
  ;; see background cache refresh:


  (do (call-get-and-print-time)
      (Thread/sleep 1500)
      (call-get-and-print-time))
  ;=> prints:
  ; cached-value-factory: creating value for key [foo]
  ; got value [value for key foo] from cache in 0.514 sec
  ; got value [value for key foo] from cache in 0.001 sec
  ; cached-value-factory: creating value for key [foo]

  ;; Notice that:
  ;; 1) the first `get` caused factory to be called
  ;; 2) the first `get` took ~500 ms
  ;; 3) After 1500 ms the `get` executed very fast
  ;; 4) Some time AFTER the second call, factory completed
  ;;    the background refresh
  ;;
  )
