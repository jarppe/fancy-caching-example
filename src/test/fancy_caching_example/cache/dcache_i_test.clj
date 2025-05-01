(ns fancy-caching-example.cache.dcache-i-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [fancy-caching-example.cache.dcache :as dcache]
            [fancy-caching-example.test-fixture :as f :refer [*pool* *client*]])
  (:import (redis.clients.jedis JedisPubSub)))


(use-fixtures :once
  (f/with-pool)
  (f/with-client)
  (fn [f]
    (dcache/load-dcache-lib *pool*)
    (f)))


;; These must match the values set in dcache.lua file:

(def dcache-miss-timeout 1000)
(def dcache-stale-timeout 1000)


(deftest dcache_get-test
  (testing "cache miss, no leader"
    (let [key       (str (gensym "key-"))
          client-id (str (gensym "client-id-"))]
      (testing "dcache_get returns status MISS"
        (is (= {"status" "MISS"} (dcache/fcall *pool* "dcache_get" key [client-id]))))
      (testing "dcache_get set us as leader"
        (is (= {"leader" client-id} (.hgetAll *client* key))))
      (testing "dcache_get set the timeout for entry"
        (let [expire-at        (.pexpireTime *client* key)
              should-expire-at (+ (System/currentTimeMillis)
                                  dcache-miss-timeout)]
          (is (< (- should-expire-at 10) expire-at (+ should-expire-at 10)))))))

  (testing "cache miss, leader exists"
    (let [key       (str (gensym "key-"))
          client-id (str (gensym "client-id-"))
          leader-id (str (gensym "leader-id-"))]
      ;; mark someone else as leader:
      (.hset *client* key "leader" leader-id)
      (testing "dcache_get returns status PENDING"
        (is (= {"status" "PENDING"} (dcache/fcall *pool* "dcache_get" key [client-id]))))))

  (testing "cache hit, value stale"
    (let [key       (str (gensym "key-"))
          client-id (str (gensym "client-id-"))]
      ;; set just the value, the absense of "fresh" indicates that this is stale:
      (.hset *client* key "value" "foo")
      (testing "dcache_get returns status PENDING"
        (is (= {"status" "STALE"
                "value"  "foo"}
               (dcache/fcall *pool* "dcache_get" key [client-id]))))
      (testing "dcache_get set us as leader"
        (is (= client-id (.hget *client* key "leader"))))
      (testing "dcache_set set the leader expiration"
        (let [should-expire-at (+ (System/currentTimeMillis)
                                  dcache-stale-timeout)
              expire-at        (-> (.hpexpireTime *client* key (into-array String ["leader"]))
                                   (.getFirst))]
          (is (< (- should-expire-at 10)
                 expire-at
                 (+ should-expire-at 10)))))))

  (testing "cache hit, fresh"
    (let [key       (str (gensym "key-"))
          client-id (str (gensym "client-id-"))]
      (.hset *client* key {"value" "foo"
                           "fresh" "t"})
      (testing "dcache_get returns status OK"
        (is (= {"status" "OK"
                "value"  "foo"}
               (dcache/fcall *pool* "dcache_get" key [client-id])))))))

(deftest dcache_set-test
  (testing "if caller is not the leader, dcache_set returns with CONFLICT"
    (let [key       (str (gensym "key-"))
          client-id (str (gensym "client-id-"))
          other-id  (str (gensym "other-id-"))]
      (.hset *client* key {"leader" other-id})
      (is (= "CONFLICT" (dcache/fcall *pool* "dcache_set" key [client-id "" "" ""])))))

  (testing "if caller is the leader, dcache_set returns with OK"
    (let [key       (str (gensym "key-"))
          client-id (str (gensym "client-id-"))
          value     "value"
          stale     100
          expire    200]
      (.hset *client* key {"leader" client-id})
      (is (= "OK" (dcache/fcall *pool* "dcache_set" key [client-id value (str stale) (str expire)])))
      (testing "value is set and it's marked as fresh"
        (is (= {"value" value
                "fresh" "t"}
               (.hgetAll *client* key))))
      (testing "entry has expiration set"
        (testing "dcache_get set the timeout for entry"
          (let [expire-at        (.pexpireTime *client* key)
                should-expire-at (+ (System/currentTimeMillis)
                                    expire)]
            (is (< (- should-expire-at 10) expire-at (+ should-expire-at 10))))))
      (testing "freshnes flas has expiration"
        (let [should-expire-at (+ (System/currentTimeMillis)
                                  stale)
              expire-at        (-> (.hpexpireTime *client* key (into-array String ["fresh"]))
                                   (.getFirst))]
          (is (< (- should-expire-at 10)
                 expire-at
                 (+ should-expire-at 10))))))))


(deftest dcache_set-notification-test
  (testing "dcache_set publishes event when value is set"
      (let [client-id (str (gensym "client-id-"))
            key       (str (gensym "key-")) 
            value     "value"
            message   (promise)
            pubsub    (proxy [JedisPubSub] []
                        (onPMessage [pattern channel event]
                          (deliver message [channel event])))
            fut       (future
                        (with-open [client (.getResource *pool*)]
                          (.psubscribe client pubsub (into-array String ["dcache_set:set:*"])))
                        ::done)]
        (Thread/sleep 10) ;; Give future some time to subscribe
        (try
          ;; Mark this as a leader:
          (.hset *client* key {"leader" client-id})
          ;; Set the value
          (is (= "OK" (dcache/fcall *pool* "dcache_set" key [client-id value "1" "2"])))
          ;; Ensure that the message was published:
          (is (= [(str "dcache_set:set:" key) value] (deref message 1000 ::timeout)))
          ;; Cleanup:
          (finally
            (.punsubscribe pubsub)
            (is (= ::done (deref fut 100 ::timeout))))))))