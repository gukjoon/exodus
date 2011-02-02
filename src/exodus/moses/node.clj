(ns exodus.moses.node
  (:require [exodus.moses.edge :as edge]
	    [work.core :as work]))


(defprotocol moses-node
  ;;public 
  ;;(grab [this] "gets most recent output of node as uri")
  (get-domain [this] "gets the domain object of node")
  (get-out-edge [this] "gets the latest uri of node. runs job if synchronous")
  ;;(get-keys [this])
  ;;(run [this])
  ;;(get-children [this])
  )

(defprotocol async-node
  (schedule [this executor runnable interval start success fail] "schedules a function"))

(deftype moses-async-node [output domain]
  async-node
  ;;schedule needs to deal with exceptions
  (schedule [this executor runnable interval start success fail]
	    (.scheduleWithFixedDelay executor
				     (work/try-job
				      (fn []
					(println start)
					(if 
					    (edge/with-out-uri output (.getTime (new java.util.Date))
					      runnable)
					  (println success)
					  (println fail))))
				      0 interval java.util.concurrent.TimeUnit/SECONDS))
  moses-node
  (get-out-edge [this] output)
  (get-domain [this] domain))
	      
	      
  				       
			 
(deftype moses-sync-node [baseuri name domain runnable]
  moses-node
  ;;file cleanup
  (get-out-edge [this]
		(let [output (new exodus.moses.edge.exodus-edge baseuri name 1 (agent {}) nil)]
		  (edge/with-out-uri output (.getTime (new java.util.Date))
		    (fn [out] (runnable out this)))
		    output))
  (get-domain [this] domain))