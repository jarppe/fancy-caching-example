(ns fancy-caching-example.cache-i-test
  (:require [fancy-caching-example.cache :as cache]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [matcher-combinators.test]
            [fancy-caching-example.test-fixture :as f :refer [*client* make-cache]]))


(use-fixtures :once (f/with-pool))
(use-fixtures :each (f/with-client))


(deftest get-test
  (testing "cache name and equality"
    (with-open [cache-1 (make-cache {:cache-name    "my special cache 1"
                                     :entry-factory (fn [k] k)})
                cache-2 (make-cache {:cache-name    "my special cache 2"
                                     :entry-factory (fn [k] k)})]
      (is (= "my special cache 1" (str cache-1)))
      (is (not= cache-1 cache-2))))

  (testing "cache via cache/get and as associative and as ifn"
    (with-open [cache (make-cache {:cache-name     (str (gensym "cache-"))
                                   :entry-factory  (fn [k] (str "value for " k))
                                   :default-expire 1})]
      (is (= "value for foo" (cache/get cache "key")))
      (is (= "value for foo" (get cache "foo")))
      (is (= "value for foo" (cache "foo")))))

  (testing "factory that returns value in map"
    (let [key (str (gensym "key-"))]
      (with-open [cache (make-cache {:cache-name    (str (gensym "cache-"))
                                     :entry-factory (fn [k] {:value  (str "value for " k)
                                                             :expire 100})})]
        (is (= (str "value for " key)
               (cache/get cache key))))))

  (testing "test staleness and expiration"
    (with-open [cache (make-cache {:cache-name    (str (gensym "cache-"))
                                   :entry-factory (fn [k] {:value  (str "value for " k)
                                                           :stale  50
                                                           :expire 100})})]
      (let [key      (str (gensym "key-"))
            value    (str "value for " key)
            full-key (str (.cache-name cache) ":" key)]
        (is (= value (cache/get cache key)))
        (testing "value is present and freshes is true"
          (is (= {"value" value
                  "fresh" "t"}
                 (.hgetAll *client* full-key))))
        (testing "after 75 ms value is still present but it's not fresh anymore"
          (Thread/sleep 75)
          (is (= {"value" value}
                 (.hgetAll *client* full-key))))
        (testing "after 125 ms value is expired"
          (Thread/sleep 50)
          (is (= false
                 (.exists *client* full-key))))))))


(deftest get-refreshing-test
  (let [cache-name (gensym "cache-")
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
        (is (= "version 1" (cache/get cache key))))

      (testing "now entry is found"
        (is (true? (.exists *client* full-key))))

      (testing "after 50ms entry still has the original value"
        (Thread/sleep 50)
        (is (= "version 1" (cache/get cache key))))

      (testing "after 150ms entry got stale, but original value was still served"
        (Thread/sleep 100)
        (is (= "version 1" (cache/get cache key))))

      (testing "the previous request was stale so a new value was generated and should be ready after a very short delay"
        (Thread/sleep 10)
        (is (= "version 2" (cache/get cache key))))

      (testing "the refreshed entry is valid for 200ms, so after 250ms the value is expired"
        (Thread/sleep 250)
        (is (false? (.exists *client* full-key))))

      (testing "when requested, a new value is generated"
        (is (= "version 3" (cache/get cache key)))))))


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
  (let [key   (str (gensym "cache-"))
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
                       (cache/get cache key)
                       ;; Should be T2
                       (is (< (T 1) (now) (T 3))))
            client-2 (future
                       ;; T0
                       (Thread/sleep (- (T 1) (T 0)))
                       ;; T1
                       (cache/get cache key)
                       ;; Should be T3
                       (is (< (T 2) (now) (T 4))))]
        @client-1
        @client-2))))

(deftest interrupted-wait-test
  (testing "client 2 goes to wait using the initial wait of 1000ms, but it should be interrupted when client 1 sets the value"
    (with-open [cache (make-cache {:cache-name     (str (gensym "cache-"))
                                   :init-wait-ms   1000
                                   :max-wait-ms    2000
                                   :default-expire 1000
                                   :entry-factory  (fn [k]
                                                     (Thread/sleep 100)
                                                     (str "value:" k))})]
      (future
        (cache/get cache "foo"))
      (let [start (System/currentTimeMillis)
            value (cache/get cache "foo")
            end   (System/currentTimeMillis)]
        (is (= "value:foo" value))
        (is (< 50 (- end start) 150)))))

  (testing "client 2 goes to wait using the initial wait of 10ms, but it should be interrupted when client 1 sets the value"
    (with-open [cache (make-cache {:cache-name     (str (gensym "cache-"))
                                   :init-wait-ms   10
                                   :max-wait-ms    10
                                   :default-expire 1000
                                   :entry-factory  (fn [k]
                                                     (Thread/sleep 100)
                                                     (str "value:" k))})]
      (future
        (cache/get cache "foo"))
      (let [start (System/currentTimeMillis)
            value (cache/get cache "foo")
            end   (System/currentTimeMillis)]
        (is (= "value:foo" value))
        (is (< 50 (- end start) 150)))))

  (testing "client 2 goes to wait using the initial wait of 10ms and should timeout before client 1 sets the value"
    (with-open [cache (make-cache {:cache-name     (str (gensym "cache-"))
                                   :init-wait-ms   10
                                   :max-wait-ms    50
                                   :default-expire 1000
                                   :entry-factory  (fn [k]
                                                     (Thread/sleep 1000)
                                                     (str "value:" k))})]
      (future
        (cache/get cache "foo"))
      (let [start (System/currentTimeMillis)
            value (cache/get cache "foo")
            end   (System/currentTimeMillis)]
        (is (= "value:foo" value))
        (is (< 50 (- end start) 150))))))
