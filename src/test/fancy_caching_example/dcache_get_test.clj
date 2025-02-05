(ns fancy-caching-example.dcache-get-test
  (:require [fancy-caching-example.stale-while-refresh :as cache]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [matcher-combinators.test]
            [fancy-caching-example.test-fixture :as f :refer [*client* make-cache]]))


(use-fixtures :once (f/with-pool))
(use-fixtures :each (f/with-client))


(def dcache-set #'cache/dcache-set)
(def dcache-get #'cache/dcache-get)


(deftest dcache-get-test
  (let [cache-name (gensym "dcache-get-test-")
        key        "key"
        full-key   (str cache-name ":" key)]
    (with-open [cache (make-cache {:cache-name    cache-name
                                   :entry-factory (fn [_] (throw (ex-info "should not be called" {})))})]
      (testing "in cold cache"
        (testing "response status is MISS"
          (is (= {"status" "MISS"} (dcache-get cache key))))
        (testing "Only the leader is set"
          (is (= {"leader" (.client-id cache)} (.hgetAll *client* full-key))))
        (testing "The hash is set to expire in 1 sec"
          (let [should-expire-at (+ (System/currentTimeMillis) 1000)]
            (is (< (- should-expire-at 10)
                   (.pexpireTime *client* full-key)
                   (+ should-expire-at 10))))))

      (testing "now entry is created and waiting for value"
        (testing "response status is PENDING"
          (is (= {"status" "PENDING"}
                 (dcache-get cache key)))))

      (dcache-set cache key {:value  "value"
                             :stale  "50"
                             :expire "150"})

      (testing "now entry is set and fresh and"
        (testing "leader is not set"
          (is (false? (.hexists *client* full-key "leader"))))
        (testing "response status is PENDING"
          (is (= {"status" "OK"
                  "value"  "value"}
                 (dcache-get cache key)))))

      (testing "after 100 ms"
        (Thread/sleep 100)
        (testing "the entry is not fresh anymore"
          (is (false? (.hexists *client* full-key "fresh"))))
        (testing "the dcache-get returns status STALE"
          (is {"status" "STALE"
               "value"  "value"}
              (dcache-get cache key)))
        (testing "leader is set with expiration of 1 sec"
          (is (= (.client-id cache) (.hget *client* full-key "leader")))
          (let [should-expire-at (+ (System/currentTimeMillis) 1000)]
            (is (< (- should-expire-at 10)
                   (-> (.hpexpireTime *client* full-key (into-array String ["leader"]))
                       (.getFirst))
                   (+ should-expire-at 10)))))))))

