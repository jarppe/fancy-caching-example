(ns fancy-caching-example.dcache-set-test
  (:require [fancy-caching-example.stale-while-refresh :as cache]
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
      (dcache-set cache key {:value  "value"
                             :stale  "50"
                             :expire "150"})
      (testing "value is available and it's fresh"
        (is (= {"value" "value"
                "fresh" "t"}
               (.hgetAll *client* full-key))))

      (Thread/sleep 100)
      (testing "after 100 ms value is still available but it's not fresh anymore"
        (let [resp (.hgetAll *client* full-key)]
          (is (= "value" (get resp "value")))
          (is (not (contains? resp "fresh")))))

      (Thread/sleep 100)
      (testing "after 200 ms value expired"
        (is (false? (.exists *client* (str "cache:" key))))))))


;;
;; Test the conflict mechanism by stealing leadership while client is
;; creating caching value. The `cache/get` should:
;;
;;   1) first client calls cache and get status "MISS"
;;   2) client starts to make a value
;;   3) before value is ready, steal leadership
;;   4) client calls dcache_set to set the value
;;   5) client detects the conflict and starts the process from start
;;   6) client gets the value set by the stealer
;;

(deftest dcache-set-conflict-test
  (let [cache-name (gensym "dcache-set-test-")
        key        "key"
        full-key   (str cache-name ":" key)]
    (with-open [cache (make-cache {:cache-name    cache-name
                                   :entry-factory (fn [_]
                                                    (Thread/sleep 200)
                                                    {:value  "client value"
                                                     :stale  1000
                                                     :expire 2000})})]
      (let [client (cache/get cache key)]
        (Thread/sleep 50)
        (.hset *client* full-key "leader" "rouge-client")
        (.hset *client* full-key "value" "rouge-value")
        (testing "The final value is the one set by rouge client"
          (is (= "roguw-value" @client)))))))
