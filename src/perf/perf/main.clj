(ns perf.main
  (:require [clojure.tools.logging :as log]
            [perf.server :as server]))


(set! *warn-on-reflection* true)


(def server nil)


(defn start []
  (alter-var-root #'server (fn [server]
                             (server/stop-server server)
                             (server/start-server))))


(defn -main [_args]
  (log/info "server statring")
  (start)
  (log/info "server ready"))


(comment
  (start)

  )