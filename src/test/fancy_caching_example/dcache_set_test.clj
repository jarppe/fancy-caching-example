(ns fancy-caching-example.dcache-set-test
  (:require [fancy-caching-example.cache :as cache]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [matcher-combinators.test]
            [fancy-caching-example.test-fixture :as f :refer [*client* make-cache]]))


(use-fixtures :once (f/with-pool))
(use-fixtures :each (f/with-client))


(def dcache-set #'cache/dcache-set)


(deftest dcache-set-test
  (let [cache-name (gensym "dcache-set-test-")
        key        "key"
        full-key   (str cache-name ":" key)]
    (with-open [cache (make-cache {:cache-name    cache-name
                                   :entry-factory (fn [_] (throw (ex-info "should not be called" {})))})]

      (testing "if the caller is *NOT* leader dcache-set returns with conflict"
        (.hset *client* full-key "leader" "not-the-right-client-id")
        (is (= "CONFLICT" (dcache-set cache key "value" 50 150))))

      (testing "if the caller is the leader dcache_set succeeds"
        (.hset *client* full-key "leader" (.client-id cache))
        (is (= "OK" (dcache-set cache key "value" 50 150)))
        (is (= {"value" "value"
                "fresh" "t"}
               (.hgetAll *client* full-key))))

      (testing "after 100 ms value is still available but it's not fresh anymore"
        (Thread/sleep 100)
        (let [resp (.hgetAll *client* full-key)]
          (is (= "value" (get resp "value")))
          (is (not (contains? resp "fresh")))))

      (testing "after 200 ms value expired"
        (Thread/sleep 100)
        (is (false? (.exists *client* full-key)))))))


;;
;; Test the conflict mechanism by stealing leadership while client is
;; creating caching value. Run the `cache/get` in separate thread. 
;; 
;; The test steps should be:
;;
;;   1) call cache/get in separate thread
;;   2) cache/get gets status "MISS"
;;   3) cache/get starts to make a value, which will take 200ms
;;   4) before cache/get makes the new value, rogue client steals the leadership
;;   5) cache/get finises making the value and calls dcache_set to set it to cache
;;   6) dcache_set detects the conflict and retures with "CONFLICT"
;;   7) cache/get starts the process from start
;;   8) cache/get client gets the value set by the rogue client
;;

(deftest dcache-set-conflict-test
  (let [cache-name (gensym "dcache-set-test-")
        key        "key"
        full-key   (str cache-name ":" key)]
    (with-open [cache (make-cache {:cache-name    cache-name
                                   :entry-factory (fn [_]
                                                    (Thread/sleep 200)
                                                    {:value "client value"})})]
      (let [value (future (cache/get cache key))]
        (Thread/sleep 50)
        (.hset *client* full-key "leader" "rogue-client")
        (.hset *client* full-key "value" "rogue-value")
        (testing "The final value is the one set by rogue client"
          (is (= "rogue-value" @value)))))))
