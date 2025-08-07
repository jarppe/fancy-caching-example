(ns jarppe.cache.util)


(defn to-msec [v]
  (if (instance? java.time.Duration v)
    (.toMillis ^java.time.Duration v)
    v))


;;
;; Helper macro over `with-open` that avoids reflection:
;;


(defmacro with-client [[sym jedis-client] & body]
  `(with-open [~(vary-meta sym assoc :tag 'redis.clients.jedis.Jedis) ~jedis-client]
     ~@body))


;;
;; Helper to work with lock objects:
;;


(defmacro with-lock [lock & body]
  (let [the-lock (vary-meta (gensym "lock-") assoc :tag 'java.util.concurrent.locks.Lock)]
    `(let [~the-lock ~lock]
       (.lock ~the-lock)
       (try
         ~@body
         (finally
           (.unlock ~the-lock))))))


;;
;; Helper to submit async tasks to executor:
;;


(defmacro async [executor & body]
  `(.execute ~(vary-meta executor assoc :tag 'java.util.concurrent.Executor)
             (^{:once true} fn* [] ~@body)))
