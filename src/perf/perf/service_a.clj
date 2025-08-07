(ns perf.service-a)


(set! *warn-on-reflection* true)


(defn handle-request [_req]
  {:status 200
   :body   "hello from A"})


(defn handler []
  (fn [req]
    (when (-> req :url (= "/a"))
      (handle-request req))))
