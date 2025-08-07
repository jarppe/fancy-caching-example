(ns jarppe.test-fixture
  (:require [clojure.string :as str]
            [jarppe.cache :as cache])
  (:import (redis.clients.jedis JedisPool
                                HostAndPort
                                DefaultJedisClientConfig)
           (java.util.concurrent Executors)))


(defmacro DEBUG [& args]
  (when (= (System/getProperty "mode") "dev")
    `(.println System/err (str "DEBUG: " (str/join " " [~@args])))))


(def ^:dynamic *pool* nil)
(def ^:dynamic *executor* nil)
(def ^:dynamic *client* nil)


(defn with-pool []
  (fn [f]
    (let [pool     (JedisPool. (HostAndPort. "redis" 6379)
                               (-> (DefaultJedisClientConfig/builder)
                                   (.clientName "fancy-caching-example-test")
                                   (.resp3)
                                   (.build)))
          executor (Executors/newVirtualThreadPerTaskExecutor)]
      (try
        (binding [*pool*     pool
                  *executor* executor]
          (f))
        (finally
          (when (not= (try
                        (deref (future 
                                 (DEBUG "Jedis pool closing")
                                 (.close pool)
                                 (DEBUG "pool closed")
                                 ::success) 
                               1000 
                               ::timeout)
                        (catch Throwable e
                          (DEBUG "WARNING: Jedis pool close exception: " e)
                          ::error))
                      ::success)
            (DEBUG "WARNING: Jedis pool refused to close")))))))


(defn with-client []
  (fn [f]
    (with-open [client (.getResource *pool*)]
      (binding [*client* client]
        (f)))))


(defn make-cache [config]
  ;; It's important to capture the pool, the `get-jedis-client` is called with async and the
  ;; dynamic bindings don't get passed there.
  (let [pool *pool*]
    (cache/init (merge {:get-jedis-client (fn [] (.getResource pool))
                        :executor         *executor*
                        :cache-name       (gensym "cache-")}
                       config))))
