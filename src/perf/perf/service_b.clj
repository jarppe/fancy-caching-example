(ns perf.service-b)


(set! *warn-on-reflection* true)


(defn handle-request [_req]
  {:status 200
   :body   "hello from B"})


(defn handler []
  (fn [req]
    (when (-> req :url (= "/b"))
      (handle-request req))))