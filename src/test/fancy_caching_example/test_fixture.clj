(ns fancy-caching-example.test-fixture
  (:require [fancy-caching-example.stale-while-refresh :as cache])
  (:import (redis.clients.jedis JedisPool
                                HostAndPort
                                DefaultJedisClientConfig)
           (java.util.concurrent Executors)))


(def ^:dynamic *pool* nil)
(def ^:dynamic *executor* nil)
(def ^:dynamic *client* nil)


(defn with-pool []
  (fn [f]
    (with-open [pool     (JedisPool. (HostAndPort. "redis" 6379)
                                     (-> (DefaultJedisClientConfig/builder)
                                         (.clientName "fancy-caching-example-test")
                                         (.resp3)
                                         (.build)))
                executor (Executors/newVirtualThreadPerTaskExecutor)]
      (binding [*pool*     pool
                *executor* executor]
        (f)))))


(defn with-client []
  (fn [f]
    (with-open [client (.getResource *pool*)]
      (binding [*client* client]
        (f)))))


(defn make-cache [factory-or-config]
  (cache/init (merge {:pool     *pool*
                      :executor *executor*}
                     (if (fn? factory-or-config)
                       {:entry-factory factory-or-config}
                       factory-or-config))))
