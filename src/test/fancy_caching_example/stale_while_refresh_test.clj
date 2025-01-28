(ns fancy-caching-example.stale-while-refresh-test
  (:require [fancy-caching-example.stale-while-refresh :as cache]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [matcher-combinators.test]
            [matcher-combinators.matchers :as m])
  (:import (redis.clients.jedis Jedis
                                JedisPool
                                HostAndPort
                                DefaultJedisClientConfig)
           (redis.clients.jedis.args FlushMode)
           (java.util.concurrent Executors)))


(def ^:dynamic *pool* nil)
(def ^:dynamic *executor* nil)
(def ^:dynamic *client* nil)


(defn with-pool []
  (fn [f]
    (with-open [pool     (JedisPool. (HostAndPort. "127.0.0.1" 6379)
                                     (-> (DefaultJedisClientConfig/builder)
                                         (.clientName "fancy-caching-example")
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


(use-fixtures :once (with-pool))
(use-fixtures :each (with-client))


(defn make-cache [factory]
  (cache/init {:entry-factory factory
               :pool          *pool*
               :executor      *executor*}))


(defn hgetall [key]
  (update-keys (.hgetAll *client* key) keyword))


(deftest get-test
  (let [entry-key (str (gensym "key-"))
        cache     (make-cache (fn [k]
                                (let [now (System/currentTimeMillis)]
                                  {:value  (str "value for " k)
                                   :stale  (+ now 100)
                                   :expire (+ now 200)})))]
    (is (= (str "value for " entry-key)
           @(cache/get cache entry-key)))))


(deftest simple-get-test
  (let [entry-key (str (gensym "key-"))
        counter   (atom 0)
        cache     (make-cache (fn [_k]
                                (let [now (System/currentTimeMillis)]
                                  {:value  (str "version " (swap! counter inc))
                                   :stale  (+ now 100)
                                   :expire (+ now 200)})))]
    (testing "initially entry is not found"
      (is (= {} (hgetall entry-key))))

    (testing "when requested, a new entry is generated"
      (is (= "version 1" @(cache/get cache entry-key))))

    (testing "after 50ms entry still has the original value"
      (Thread/sleep 50)
      (is (= "version 1" @(cache/get cache entry-key))))

    (testing "after 150ms entry got stale, but original value was still served"
      (Thread/sleep 100)
      (is (= "version 1" @(cache/get cache entry-key))))

    (testing "the previous request was stale so a new value was generated and should be ready after a very short delay"
      (Thread/sleep 10)
      (is (= "version 2" @(cache/get cache entry-key))))

    (testing "the refreshed entry is valid for 200ms, so after 250ms the value is expired"
      (Thread/sleep 250)
      (is (= {} (hgetall entry-key))))

    (testing "when requested, a new value is generated"
      (is (= "version 3" @(cache/get cache entry-key))))))


(deftest stampede-get-test
  (let [entry-key "foo"
        counter   (atom 0)
        cache     (make-cache (fn [k]
                                (let [now (System/currentTimeMillis)]
                                  {:value  (str k ": " (swap! counter inc))
                                   :stale  (+ now 100)
                                   :expire (+ now 200)})))]
    (testing "initially entry is not found"
      (is (= {} (hgetall entry-key))))

    (testing "when requested, a new entry is generated"
      (is (= "foo: 1" @(cache/get cache entry-key))))

    (testing "after 50ms entry still has the original value"
      (Thread/sleep 50)
      (is (= "foo: 1" @(cache/get cache entry-key))))

    (testing "after 150ms entry got stale, but original value was still served"
      (Thread/sleep 100)
      (is (= "foo: 1" @(cache/get cache entry-key))))

    (testing "the previous request was stale so a new value was generated and should be ready after a very short delay"
      (Thread/sleep 10)
      (is (= "foo: 2" @(cache/get cache entry-key))))

    (testing "the refreshed entry is valid for 200ms, so after 250ms the value is expired"
      (Thread/sleep 250)
      (is (= {} (hgetall entry-key))))

    (testing "when requested, a new value is generated"
      (is (= "foo: 3" @(cache/get cache entry-key))))))
