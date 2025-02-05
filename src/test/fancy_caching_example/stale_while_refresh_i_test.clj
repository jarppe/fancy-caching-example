(ns fancy-caching-example.stale-while-refresh-i-test
  (:require [fancy-caching-example.stale-while-refresh :as cache]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [matcher-combinators.test]
            [fancy-caching-example.test-fixture :as f :refer [*client* make-cache]]))


(use-fixtures :once (f/with-pool))
(use-fixtures :each (f/with-client))


(deftest get-test
  (with-open [cache (make-cache (fn [k]
                                  {:value  (str "value for " k)
                                   :stale  100
                                   :expire 200}))]
    (let [entry-key (str (gensym "get-test-"))]
      (is (= (str "value for " entry-key)
             @(cache/get cache entry-key))))))


(deftest simple-get-test
  (let [cache-name (gensym "dcache-get-test-")
        key        "key"
        full-key   (str cache-name ":" key)
        counter    (atom 0)]
    (with-open [cache (make-cache {:cache-name    cache-name
                                   :entry-factory (fn [_k]
                                                    {:value  (str "version " (swap! counter inc))
                                                     :stale  100
                                                     :expire 200})})]
      (testing "initially entry is not found"
        (is (false? (.exists *client* full-key))))

      (testing "when requested, a new entry is generated"
        (is (= "version 1" @(cache/get cache key))))

      (testing "now entry is not found"
        (is (true? (.exists *client* full-key))))

      (testing "after 50ms entry still has the original value"
        (Thread/sleep 50)
        (is (= "version 1" @(cache/get cache key))))

      (testing "after 150ms entry got stale, but original value was still served"
        (Thread/sleep 100)
        (is (= "version 1" @(cache/get cache key))))

      (testing "the previous request was stale so a new value was generated and should be ready after a very short delay"
        (Thread/sleep 10)
        (is (= "version 2" @(cache/get cache key))))

      (testing "the refreshed entry is valid for 200ms, so after 250ms the value is expired"
        (Thread/sleep 250)
        (is (false? (.exists *client* full-key))))

      (testing "when requested, a new value is generated"
        (is (= "version 3" @(cache/get cache key)))))))


(deftest stampede-get-test
  (let [cache-name (gensym "dcache-get-test-")
        key        "key"
        full-key   (str cache-name ":" key)
        counter    (atom 0)]
    (with-open [cache (make-cache {:cache-name    cache-name
                                   :entry-factory (fn [k]
                                                    {:value  (str k ":" (swap! counter inc))
                                                     :stale  100
                                                     :expire 200})})]
      (testing "initially entry is not found"
        (is (false? (.exists *client* full-key))))

      (testing "when requested, a new entry is generated"
        (is (= (str key ":1") @(cache/get cache key))))

      (testing "now entry is found"
        (is (true? (.exists *client* full-key))))

      (testing "after 50ms entry still has the original value"
        (Thread/sleep 50)
        (is (= (str key ":1") @(cache/get cache key))))

      (testing "after 150ms entry got stale, but original value was still served"
        (Thread/sleep 100)
        (is (= (str key ":1") @(cache/get cache key))))

      (testing "the previous request was stale so a new value was generated and should be ready after a very short delay"
        (Thread/sleep 10)
        (is (= (str key ":2") @(cache/get cache key))))

      (testing "the refreshed entry is valid for 200ms, so after 250ms the value is expired"
        (Thread/sleep 250)
        (is (false? (.exists *client* full-key))))

      (testing "when requested, a new value is generated"
        (is (= (str key ":3") @(cache/get cache key)))))))


;; T0:  client 1 ----GET-----> cache
;;      client 1 <---MISS----- cache
;;
;; T1:  client 2 ----GET-----> cache
;;      client 2 <---PENDING-- cache
;;
;; T2:  client 1 ----SET-----> cache
;;
;; T3:  client 2 ----GET-----> cache
;;      client 2 <---HIT------ cache


(deftest long-wait-test
  (let [key   (str (gensym "long-wait-test-"))
        start (System/currentTimeMillis)
        now   (fn [] (- (System/currentTimeMillis) start))
        t'    20
        T     (fn [t] (* t' t))]
    (with-open [cache (make-cache {:init-wait-ms  (- (T 3) (T 1))
                                   :max-wait-ms   (- (T 3) (T 1))
                                   :entry-factory (fn [k]
                                                    (Thread/sleep (- (T 2) (T 0)))
                                                    {:value  (str "value:" k)
                                                     :stale  1000
                                                     :expire 1000})})]
      (let [client-1 (future
                       ;; T0
                       @(cache/get cache key)
                       ;; Should be T2
                       (is (< (T 1) (now) (T 3))))
            client-2 (future
                       ;; T0
                       (Thread/sleep (- (T 1) (T 0)))
                       ;; T1
                       @(cache/get cache key)
                       ;; Should be T3
                       (is (< (T 2) (now) (T 4))))]
        @client-1
        @client-2))))
