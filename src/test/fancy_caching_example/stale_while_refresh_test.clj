(ns fancy-caching-example.stale-while-refresh-test
  (:require [fancy-caching-example.stale-while-refresh :as cache]
            [clojure.test :refer [deftest are]]))


(def get-state #'cache/get-state)


(deftest get-state-test
  (are [expected item] (= (get-state item 100) expected)
    :pending
    {:pending? true}

    :fresh
    {:pending?    false
     :expire-at   150
     :refresh-ttl 40}

    :mature
    {:pending?    false
     :expire-at   150
     :refresh-ttl 60}

    :stale
    {:pending?    false
     :expire-at   80
     :refresh-ttl 10}))