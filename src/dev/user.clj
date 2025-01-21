(ns user
  (:require [clojure.tools.namespace.repl :as tnr]))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start []
  (println "system starting...")
  "System up")


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn reset []
  (println "system resetting...")
  (tnr/refresh :after 'user/start))


(comment
  (start)
  ;
  )