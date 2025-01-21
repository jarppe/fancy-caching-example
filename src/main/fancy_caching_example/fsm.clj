(ns fancy-caching-example.fsm)

(defn apply-signal [fsm signal & args]
  (let [current-state (-> fsm :state)]))