(ns fancy-caching-example.test-fixture
  (:require [clojure.string :as str]
            [fancy-caching-example.cache :as cache])
  (:import (redis.clients.jedis JedisPool
                                HostAndPort
                                DefaultJedisClientConfig)
           (java.util.concurrent Executors)))


(defmacro DEBUG [& args]
  (when (= (System/getProperty "mode") "dev")
    `(.println System/err (str "DEBUG: [" (.getName *ns*) ":" ~(:line (meta &form)) "] " (str/join " " ~(vec args))))))



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
                                 (DEBUG "test-fixture: Jedis pool closing")
                                 (.close pool)
                                 (DEBUG "test-fixture: pool closed")
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
  (cache/init (merge {:pool       *pool*
                      :executor   *executor*
                      :cache-name (gensym "cache-")}
                     config)))
