(ns perf.server
  (:require [ring.adapter.jetty9 :as jetty]
            [perf.service-a :as a]
            [perf.service-b :as b]
            [perf.control :as control]))


(set! *warn-on-reflection* true)


(defn start-server []
  (jetty/run-jetty (some-fn (a/handler)
                            (b/handler)
                            (control/handler)
                            (constantly {:status 404
                                         :body   "I dont' even"}))
                   {:host                 "0.0.0.0"
                    :port                 8080
                    :join?                false
                    :virtual-threads?     true
                    :allow-null-path-info true}))


(defn stop-server [server]
  (when server 
    (jetty/stop-server server)))
