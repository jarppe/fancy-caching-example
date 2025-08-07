(ns jarppe.cache-i-test
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [matcher-combinators.test]
            [jarppe.test-fixture :as f :refer [*client* make-cache]]))


(use-fixtures :once (f/with-pool))
(use-fixtures :each (f/with-client))


(deftest cache-name-test
  (let [cache-name (str (gensym "cache-"))
        client-id  (str (gensym "client-id-"))]
    (with-open [cache (make-cache {:cache-name    cache-name
                                   :client-id     client-id
                                   :entry-factory identity})]
      (is (= cache-name (name cache)))
      (is (= (str "Cache[cache-name=\"" cache-name "\",client-id=\"" client-id "\"]") (str cache))))))


(deftest cache-equality-test
  (with-open [cache-1 (make-cache {:entry-factory identity})
              cache-2 (make-cache {:entry-factory identity})]
    (is (not= cache-1 cache-2))))


(deftest cache-as-map-test
  (with-open [cache (make-cache {:entry-factory  str/upper-case
                                 :default-expire 1})]
    (is (= "FOO 1" (get cache "foo 1")))
    (is (= "FOO 2" (get cache "foo 2" 'ignored)))))


(deftest cache-as-fn-test
  (with-open [cache (make-cache {:entry-factory  str/upper-case
                                 :default-expire 1})]
    (is (= "FOO 1" (cache "foo 1")))
    (is (= "FOO 2" (cache "foo 2" 100)))
    (is (= "FOO 3" (cache "foo 3" 100 :timeout)))))


(deftest encode-decode-test
  ;; Note that the encode and decode used in this test are *NOT* valid in general
  ;; sense, as the decode and encode MUST always work so that:
  ;;
  ;;    (is (= x (decode (encode x))))
  ;;
  ;; is true for all values of `x`.
  ;;
  ;; For testing purposes the encode and decode used in this test do not comply with
  ;; this.
  (with-open [cache (make-cache {:entry-factory  str/upper-case
                                 :default-expire 1
                                 :encode         (fn [v] (str "encode:" v))
                                 :decode         (fn [v] (str "decode:" v))})]
    ;; first call is MISS so call returns what ever factory returned:
    (is (= "FOO" (cache "foo")))
    ;; second call is HIT so call returns value first written to Redis via
    ;; encode and then read from Redis via decode: 
    (is (= "decode:encode:FOO" (cache "foo"))))

  ;; An example of better use of encode/decode. Still, it should be noted that pr-str 
  ;; does not produce EDN with all values, for more info see this
  ;; https://nitor.com/en/articles/pitfalls-and-bumps-clojures-extensible-data-notation-edn
  (with-open [cache (make-cache {:entry-factory  (fn [k]
                                                   {:value (str/upper-case k)})
                                 :encode         pr-str
                                 :decode         edn/read-string
                                 :default-expire 1})]
    (is (= {:value "FOO"} (cache "foo")))
    (is (= {:value "FOO"} (cache "foo")))))


(deftest expire-missing-test
  (testing "no default expire and :expire not provided by factory causes an exception"
    (with-open [cache (make-cache {:entry-factory str/upper-case})]
      (try
        (cache "foo")
        (is false "should throw")
        (catch clojure.lang.ExceptionInfo e
          (is (= (ex-message e) "expiration time missing")))))))


(deftest factory-provides-expire-test
  (testing "factory returns expiration in metadata"
    (with-open [cache (make-cache {:entry-factory (fn [k]
                                                    ^{:expire 1} [(str/upper-case k)])
                                   :encode        pr-str
                                   :decode        edn/read-string})]
      (is (= ["FOO"] (cache "foo")))
      (is (= ["FOO"] (cache "foo"))))))


(deftest use-default-expire-test
  (testing "default expiration provided"
    (with-open [cache (make-cache {:entry-factory  str/upper-case
                                   :default-expire 1})]
      (is (= "FOO" (cache "foo"))))))


(deftest staleness-and-expiration-test
  (with-open [cache (make-cache {:entry-factory (fn [k]
                                                  (with-meta
                                                    [k]
                                                    {:stale  50
                                                     :expire 100}))
                                 :encode        pr-str
                                 :decode        edn/read-string})]
    (let [full-key (str (.cache-name cache) ":" "foo")]
      (testing "value is stored in cache"
        (is (= ["foo"] (cache "foo"))))
      (testing "value is present in Redis and freshes is set to true"
        (is (= {"value" "[\"foo\"]"
                "fresh" "t"}
               (.hgetAll *client* full-key))))
      (testing "decode convert edn string"
        (is (= ["foo"] (cache "foo"))))
      (testing "after 75 ms value is still present but it's not fresh anymore"
        (Thread/sleep 75)
        (is (= {"value" "[\"foo\"]"}
               (.hgetAll *client* full-key))))
      (testing "after 125 ms value is expired"
        (Thread/sleep 50)
        (is (= false
               (.exists *client* full-key)))))))


(deftest get-refreshing-test
  (let [cache-name (name (gensym "cache-"))
        key        "key"
        full-key   (str cache-name ":" key)
        counter    (atom 0)]
    (with-open [cache (make-cache {:cache-name     cache-name
                                   :entry-factory  (fn [_k] (str "version " (swap! counter inc)))
                                   :default-stale  100
                                   :default-expire 200})]
      (testing "initially entry is not in Redis"
        (is (false? (.exists *client* full-key))))

      (testing "when requested, a new entry is generated"
        (is (= "version 1" (cache key))))

      (testing "now entry is found"
        (is (true? (.exists *client* full-key))))

      (testing "after 50ms entry still has the original value"
        (Thread/sleep 50)
        (is (= "version 1" (cache key))))

      (testing "after 150ms entry got stale, but original value was still served"
        (Thread/sleep 100)
        (is (= "version 1" (cache key))))

      (testing "the previous request was stale so a new value was generated and should be ready after a very short delay"
        (Thread/sleep 10)
        (is (= "version 2" (cache key))))

      (testing "the refreshed entry is valid for 200ms, so after 250ms the value is expired"
        (Thread/sleep 250)
        (is (false? (.exists *client* full-key))))

      (testing "when requested, a new value is generated"
        (is (= "version 3" (cache key)))))))


;; Timeline for the long-wait-test:
;;
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
;;
;; TODO: Figure out a better way to test this

(deftest long-wait-test
  (let [key   (str (gensym "key-"))
        start (System/currentTimeMillis)
        now   (fn [] (- (System/currentTimeMillis) start))
        t'    20
        T     (fn [t] (* t' t))]
    (with-open [cache (make-cache {:cache-name     (gensym "cache-")
                                   :init-wait      (- (T 3) (T 1))
                                   :max-wait       (- (T 3) (T 1))
                                   :entry-factory  (fn [k]
                                                     (Thread/sleep (- (T 2) (T 0)))
                                                     (str "value:" k))
                                   :default-stale  1000
                                   :default-expire 1000})]
      (let [client-1 (future
                       ;; T0
                       (cache key)
                       ;; Time should be T2
                       (is (< (T 1) (now) (T 3))))
            client-2 (future
                       ;; T0
                       (Thread/sleep (- (T 1) (T 0)))
                       ;; T1
                       (cache key)
                       ;; Time should be T3
                       (is (< (T 2) (now) (T 4))))]
        @client-1
        @client-2))))

(deftest interrupted-wait-test
  (testing "client 2 goes to wait using the initial wait of 1000ms, but it should be interrupted when client 1 sets the value"
    (with-open [cache (make-cache {:entry-factory  (fn [k]
                                                     (Thread/sleep 100)
                                                     (str "value:" k))
                                   :default-expire 1000
                                   :init-wait      1000
                                   :max-wait       2000})]
      ;; Client 1, factory called, after 100ms the value should be set in Redis:
      (future
        (cache "foo"))

      ;; Make sure the Client 1 has started and is marked as leader:
      (Thread/sleep 5)

      ;; Client 2, dcache_get returns "PENDING" causing the client 2 to wait for
      ;; leader. Without notification this would take `init-wait-ms` (1000ms), but
      ;; client 2 should get notification immediatelly after the leader has produced
      ;; a value.
      (let [start (System/currentTimeMillis)
            value (cache "foo")
            end   (System/currentTimeMillis)]
        (is (= "value:foo" value) "corret value was returned")
        (is (< 50 (- end start) 150) "value was received ~100 ms"))))

  (testing "client 2 goes to wait using the initial wait of 10ms, but it should be interrupted when client 1 sets the value"
    (with-open [cache (make-cache {:entry-factory  (fn [k]
                                                     (Thread/sleep 100)
                                                     (str "value:" k))
                                   :default-expire 1000
                                   :init-wait      10
                                   :max-wait       10})]

      ;; Client 1, factory called, after 100ms the value should be set in Redis:
      (future
        (cache "foo"))

      ;; Make sure the Client 1 has started and is marked as leader:
      (Thread/sleep 5)

      ;; Client 2, dcache_get returns "PENDING" causing the client 2 to wait for
      ;; leader. This should wait for notification or `init-wait-ms` (10ms). This
      ;; test should cause the timeout on wait multiple times before the value
      ;; is available.
      (let [start (System/currentTimeMillis)
            value (cache "foo")
            end   (System/currentTimeMillis)]
        (is (= "value:foo" value) "corret value was returned")
        (is (< 50 (- end start) 150) "value was received ~100 ms")))))
