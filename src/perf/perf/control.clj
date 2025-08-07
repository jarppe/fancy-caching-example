(ns perf.control)


(set! *warn-on-reflection* true)


(defn handle-request [_req]
  {:status 200
   :body   "hello from control"})


(defn handler []
  (fn [req]
    (when (-> req :url (= "/control"))
      (handle-request req))))
