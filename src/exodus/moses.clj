(ns exodus.moses
  (:require [exodus.moses.edge :as edge]
	    [exodus.moses.node :as node]
	    [exodus.operations :as ops])
  (:use exodus.common.tools))

(def *dir* "/exodus")

(def *expiration* 100000)

(def *executor*
     (java.util.concurrent.Executors/newScheduledThreadPool 4))

(def *asyncs*
     (vec '(http-pollster db-pollster fake-producer
			  hashjoiner mergejoiner
			  multijoiner prioritizer)))

;;globals
(defmacro dataflow [name bindings & body]
  (let [*dir* (str "/exodus" name)
	*name* (str name)
	*expiration* 100000
	*async-count* `(form-search ~bindings *asyncs*)
	*executor* (java.util.concurrent.Executors/newScheduledThreadPool
		   *async-count*)]
    `(let ~bindings
       ~@body)))


(defn http-pollster [uri domain interval]
  (let [out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/httppollster-" (gensym))
		      (gensym)
		      *expiration* (agent {}) nil)
	mnode (new exodus.moses.node.moses-async-node out-edge domain)]
    (node/schedule mnode *executor*
		   (fn [out]
		     (ops/http-fetch uri out false)) interval
		     (str "Polling " uri)
		     (str "Done polling " uri)
		     (str "Failed to poll " uri))
    mnode))

(defn db-pollster [db query domain interval]
  (let [out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/dbpollster-" (gensym))
		      (gensym)
		      *expiration* (agent {}) nil)
	mnode (new exodus.moses.node.moses-async-node out-edge domain)]
    (node/schedule mnode *executor*
		   (fn [out]
		     (ops/db-fetch db query out)) interval
		     (str "Polling database " db " with query " query)
		     (str "Done polling database " db)
		     (str "Failed to poll database " db))
    mnode))

(defn db-fetcher [db query domain]
  (let [mnode (new exodus.moses.node.moses-sync-node
		   (str *dir* "/dbfetcher-" (gensym))
		   (gensym)
		   domain
		   (fn [out caller]
		     (println (str "Querying " db " with query: " query ", called from: " caller))
		     (ops/db-fetch db query out)))]
    mnode))

(defn static-dir [uri domain]
  (let [out-edge (new exodus.moses.edge.static-edge
		      uri)
	mnode (new exodus.moses.node.moses-async-node out-edge domain)]
    mnode))


(defn fake-producer [keyvec valvec domain interval]
  (let [out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/fake-" (gensym))
		      (gensym)
		      *expiration* (agent {}) nil)
	mnode (new exodus.moses.node.moses-async-node out-edge domain)]
    (node/schedule mnode *executor*
		   (fn [out]
		     (ops/gen-fake keyvec valvec out))
		   interval
		   "Generating fake data..."
		   "Finished generating fake data..."
		   "Failed to generate fake data...")
    mnode))
			     
			     

(defn- domain-to-map [domain]
  (zipmap domain (range (count domain))))
  
;;if scheduled process fails, file gets added anyways
(defn hashjoiner [root-node config-nodes values interval & opts]
  "root-node => node
   config-nodes [node key priority/type ...]
   values => [value-key...]
   interval => int
   & opts => :prioritize or not
   Creates and returns an asynchronous hashjoiner"
  (let [prioritize (= (first opts) :prioritize)
	config-nodes (partition 3 config-nodes)
	out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/hashjoin-" (gensym))
		      (gensym)
		      *expiration* (agent {}) "/part*" )
	mnode (new exodus.moses.node.moses-async-node out-edge
		   (if prioritize
		     (conj (apply conj (node/get-domain root-node) values) :priority)
		     (apply conj (node/get-domain root-node) values)))
	config-opts (map #(let [x (rest %)]
			    (list (first x) (cond
					     (= (second x) :filter) 0
					     (= (second x) :join) 1
					     :else (second x)))) config-nodes)]
    (node/schedule mnode *executor*
		   (fn [out]
		     (edge/with-latest [root-latest (node/get-out-edge root-node)
					config-latest  (vec (map #(node/get-out-edge (first %)) config-nodes))]
		       (let [fullconfig (vec (map #(vec (conj (second %) (first %))) (zipmap config-latest config-opts)))]
			 (println root-latest)
			 (println fullconfig)
			 (ops/hashjoin root-latest
				       fullconfig
				       out
				       (domain-to-map (node/get-domain root-node))
				       prioritize)))) interval
				       "Performing hashjoin on ..."
				       "Finished with hashjoin"
				       "Hashjoin failed")
    mnode))

(defn multijoiner [root-node config-nodes join interval & opts]
  "root-node => node
   config-nodes => [node values-used ...]
   join => [key...]
   values => [value-key...]
   interval => int
   & opts => :outer or not
   
   Creates an asynchronous multijoiner. Performs a left join from root-node to each of the config-nodes.
   Each config-node is considered a separate table."
  (let [outer (= (first opts) :outer)
	config-objs (partition 2 config-nodes)
	out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/mergejoin-" (gensym))
		      (gensym)
		      *expiration* (agent {}) "/part*")
	config-nodes (map first config-objs)
	config-domains (map #(domain-to-map (node/get-domain %)) config-nodes)
	config-vals (map second config-objs)
	mnode (new exodus.moses.node.moses-async-node out-edge
		   (reduce (fn [x y] (apply conj x y)) (node/get-domain root-node) config-vals))]
    (node/schedule mnode *executor*
		   (fn [out]
		     (edge/with-latest [root-latest (node/get-out-edge root-node)
					config-latest (vec (map node/get-out-edge config-nodes))]
		       (let [fullconfig (zipvec config-latest config-vals config-domains)]
			 (ops/multijoin root-latest
					fullconfig
					out
					(domain-to-map (node/get-domain root-node))
					join
					outer)))) interval
					"Performing multijoin "
					"Finished with multijoin "
					"Multijoin failed ")
    mnode))


(defn mergejoiner [root-node config-nodes join values interval]
  "root-node => node
   config-nodes [node...]
   join => [key...]
   values => [value-key...]
   interval => int
   Creates an asynchronous merge joiner"
  (let [config-nodes (partition 2 config-nodes)
	out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/mergejoin-" (gensym))
		      (gensym)
		      *expiration* (agent {}) "/part*")
	mnode (new exodus.moses.node.moses-async-node out-edge
		   (conj (apply conj (node/get-domain root-node) values) :priority))
	config-opts (map second config-nodes)]
    (node/schedule mnode *executor*
		   (fn [out]
		     (edge/with-latest [root-latest (node/get-out-edge root-node)
					config-latest (vec (map #(node/get-out-edge (first %)) config-nodes))]
		       (let [fullconfig (vec (map #(vector (first %) (second %)) (zipmap config-latest config-opts)))]
			 (println root-latest)
			 (println fullconfig)
			 (ops/mergejoin root-latest
					fullconfig
					out
					(domain-to-map (node/get-domain root-node))
					join)))) interval
					"Performing mergejoin"
					"Finished with mergejoin"
					"Mergejoin failed")
    mnode))



(defn prioritizer [inputs keys values interval]
  "inputs => [node...]
   keys => [key...]
   values => [value-key...]
   interval => int
   Creates and returns an asynchronous prioritizer"
  (assert-args prioritizer
	       (vector? inputs)
	       "inputs must be a vector"
	       (reduce (fn [x y] (and x y))
		       (map #(or (instance? exodus.moses.node.moses-async-node %)
				 (instance? exodus.moses.node.moses-sync-node %)) inputs))
	       "inputs must be a vector of nodes"
	       (apply = (map #(node/get-domain %) inputs))
	       "domain must be homogenous"
	       (contains? (domain-to-map (node/get-domain (first inputs))) :priority)
	       "domain must contain :priority key"
	       (reduce (fn [x y] (and x y))
		       (map #(contains? (domain-to-map (node/get-domain (first inputs))) %) (apply conj keys values)))
	       "keys and values must be in input domain")
  ;;verify homogeneity of domain
  (let [out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/prioritize-" (gensym))
		      (gensym)
		      *expiration* (agent {}) "/part*")
	domain (apply conj keys values)
	indomain (domain-to-map (node/get-domain (first inputs)))
	mnode (new exodus.moses.node.moses-async-node out-edge domain)]
    (println indomain)
    (node/schedule mnode *executor*
		   (fn [out]
		     (edge/with-latest [input-latest (vec (map #(node/get-out-edge %) inputs))]
		       (ops/prioritize input-latest
				       out
				       indomain
				       keys
				       values))) interval
				       "Performing prioritization"
				       "Prioritization succeeded"
				       "Prioritization failed")
    mnode))

(defn pipeline-mapper [inputs pipelines pipeoutputs interval]
  "inputs => [node...]
   pipelines => {str-name fn-str} or fn-str
   pipeoutputs => [key...]
   interval => int
   
   Creates an asynchronous pipeline-mapper"
  (assert-args pipeline-mapper
	       (vector? inputs)
	       "inputs must be a vector"
	       (reduce (fn [x y] (and x y))
		       (map #(or (instance? exodus.moses.node.moses-async-node %)
				 (instance? exodus.moses.node.moses-sync-node %)) inputs))
	       "inputs must be a vector of nodes"
	       (apply = (map #(node/get-domain %) inputs))
	       "domain must be homogenous"
	       (or (and (map? pipelines)
			(contains? (domain-to-map (node/get-domain (first inputs))) :pipeline))
		   (string? pipelines))
	       "domain must contain pipeline key with multiple pipelines")
  (let [out-edge (new exodus.moses.edge.exodus-edge
		      (str *dir* "/pipeline-" (gensym))
		      (gensym)
		      *expiration* (agent {}) "/part*")
	indomain (node/get-domain (first inputs))
	domain (apply conj indomain pipeoutputs)
	mnode (new exodus.moses.node.moses-async-node out-edge domain)]
    (node/schedule mnode *executor*
		   (fn [out]
		     (edge/with-latest [input-latest (vec (map #(node/get-out-edge %) inputs))]
		       (ops/pipeline-execute input-latest
					     out
					     pipelines
					     (domain-to-map domain)))) interval
					     "Performing pipeline mapping"
					     "Pipeline mapping complete"
					     "Pipeline mapping failed")
    mnode))