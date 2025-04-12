(ns fancy-caching-example.cache.notification
  (:import (java.util List
                      LinkedList)
           (java.util.function Supplier)
           (java.util.concurrent Executor
                                 CompletableFuture)
           (java.util.concurrent.locks Lock
                                       ReentrantLock)
           (redis.clients.jedis JedisPool
                                JedisPubSub) 
           (clojure.lang MapEntry)))


(set! *warn-on-reflection* true)


;;
;; Helpers:
;;


(defn- supply-async ^CompletableFuture [^Executor executor ^Supplier task]
  (CompletableFuture/supplyAsync task executor))


(def ^:private ^String/1 dcache-set-channel-name (into-array String ["dcache_set:set"]))


(defmacro ^:private with-lock [lock & body]
  (let [the-lock (vary-meta (gensym "lock-") assoc :tag `Lock)]
    `(let [~the-lock ~lock]
       (.lock ~the-lock)
       (try
         ~@body
         (finally
           (.unlock ~the-lock))))))


;;
;; Support for interrupting wait with Redis pub-sub messages:
;;


(defrecord NotificationListeners [^List listeners ^java.util.concurrent.locks.Lock listeners-lock])


(defn notification-listeners ^NotificationListeners []
  (NotificationListeners. (LinkedList.) (ReentrantLock.)))


(defn wait-notification [^NotificationListeners notification-listeners cache-key timeout]
  (let [^List listeners (.listeners notification-listeners)
        listeners-lock  (.listeners-lock notification-listeners)
        p               (promise)
        entry           (MapEntry. cache-key p)]
    (with-lock listeners-lock
      (.add listeners entry))
    (deref p timeout ::ignored)
    (with-lock listeners-lock
      (.remove listeners entry))))


(defn deliver-notification [^NotificationListeners notification-listeners cache-key]
  (let [^List listeners (.listeners notification-listeners)
        listeners-lock  (.listeners-lock notification-listeners)]
    (with-lock listeners-lock
      (let [iter (.listIterator listeners)]
        (while (.hasNext iter)
          (let [entry (.next iter)]
            (when (= (key entry) cache-key)
              (deliver (val entry) cache-key)
              (.remove iter))))))))


(defrecord NotificationSubscription [on-close]
  java.io.Closeable
  (close [_] (on-close)))


(defn notification-subscription ^NotificationSubscription [cache-name executor pool listeners]
  (let [pubsub     (let [cache-prefix-len (inc (count cache-name))]
                     (proxy [JedisPubSub] []
                       (onMessage [_ message]
                         (let [cache-key (subs message cache-prefix-len)]
                           (deliver-notification listeners cache-key)))))
        pubsub-fut (supply-async executor (fn []
                                            (try
                                              (with-open [client (.getResource ^JedisPool pool)]
                                                (println "pubsub: subscribe...")
                                                (.subscribe client pubsub dcache-set-channel-name)
                                                (println "pubsub: subscribe exit"))
                                              (catch Exception e
                                                (println "pubsub: error" e)
                                                (throw e)))))
        on-close   (fn []
                     (.unsubscribe pubsub)
                     (.get pubsub-fut))]
    (NotificationSubscription. on-close)))
