(ns fancy-caching-example.cache.notification
  (:import (java.util List
                      LinkedList) 
           (java.util.concurrent Executor)
           (java.util.concurrent.locks Lock
                                       ReentrantLock)
           (redis.clients.jedis JedisPool
                                JedisPubSub) 
           (clojure.lang MapEntry)))


(set! *warn-on-reflection* true)


;;
;; Helpers:
;;


(def ^:private dcache-channel "dcache_set:set")
(def ^:private dcache-channel-prefix-len (count (str dcache-channel ":")))


(defmacro ^:private with-lock [lock & body]
  (let [the-lock (vary-meta (gensym "lock-") assoc :tag `Lock)]
    `(let [~the-lock ~lock]
       (.lock ~the-lock)
       (try
         ~@body
         (finally
           (.unlock ~the-lock))))))


(defmacro ^:private async [executor & body]
  `(.execute ~(vary-meta executor assoc :tag `Executor) 
             (^{:once true} fn* [] ~@body)))


(defn- notification-deliverer [^List listeners listeners-lock] 
  (fn [cache-key cache-value]
    (with-lock listeners-lock
      (let [iter (.listIterator listeners)]
        (while (.hasNext iter)
          (let [entry (.next iter)]
            (when (= (key entry) cache-key)
              (deliver (val entry) cache-value)
              (.remove iter))))))))


(defn- pubsub-handler ^JedisPubSub [cache-name on-message]
  (let [cache-name-len  (count cache-name)
        cache-key-index (+ dcache-channel-prefix-len 1 cache-name-len)]
    (proxy [JedisPubSub] []
      (onPMessage [_pattern channel message]
        (let [cache-key (subs channel cache-key-index)]
          (on-message cache-key message))))))


;;
;; Support for interrupting wait with Redis pub-sub messages:
;;


(defn notification-listener [{:keys [cache-name executor pool]}]
  (let [listeners      (LinkedList.)
        listeners-lock (ReentrantLock.)
        on-message     (notification-deliverer listeners listeners-lock)
        pubsub         (pubsub-handler cache-name on-message)
        pattern        (str dcache-channel ":" cache-name ":*")
        pattern-array  (into-array String [pattern])
        run            (volatile! true)]
      (async executor (while @run 
                        (try 
                          (with-open [client (.getResource ^JedisPool pool)] 
                            (.psubscribe client pubsub ^String/1 pattern-array)) 
                          (catch Exception e 
                            (.println System/err (str "WARNING: notification-listeners/subscription: exception: "
                                                      (-> e (.getClass) (.getName))
                                                      ": "
                                                      (-> e (.getMessage)))) 
                            (Thread/sleep 1000)))))
    {:listeners      listeners
     :listeners-lock listeners-lock
     :close          (fn []
                       (vreset! run false)
                       ;; The punsubscribe gets stuck sometimes. This should not be a big problem in practice
                       ;; as this is typically called only when the app is terminating. 
                       (async executor (.punsubscribe pubsub)))}))


(defn wait-notification [{:keys [^List listeners listeners-lock]} cache-key timeout]
  (let [p     (promise)
        entry (MapEntry. cache-key p)]
    (with-lock listeners-lock
      (.add listeners entry))
    (deref p timeout ::ignored)
    (with-lock listeners-lock
      (.remove listeners entry))))


(defn close [{:keys [close]}]
  (close))
