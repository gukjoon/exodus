(ns exodus.operations.mapreduce.multijoin
  (:require [clojure.contrib.duck-streams :as duck]
	    [clojure.contrib.str-utils :as str])
  (:use exodus.common.tools)
  (:import (java.util StringTokenizer)
	   (java.net URI)
           (org.apache.hadoop.util Tool)
	   (org.apache.hadoop.io LongWritable Text NullWritable)
	   (org.apache.hadoop.fs Path)
	   (org.apache.hadoop.mapreduce Mapper Mapper$Context Reducer Reducer$Context Job)
	   (org.apache.hadoop.mapreduce.lib.input TextInputFormat FileInputFormat FileSplit)
	   (org.apache.hadoop.mapreduce.lib.output TextOutputFormat FileOutputFormat)))

;;gen-class directives
;;see http://clojure.org/compilation for documentation
(gen-class
 :name exodus.operations.mapreduce.multijoin.tool
 :extends "org.apache.hadoop.conf.Configured"
 :implements ["org.apache.hadoop.util.Tool"]
 :prefix "tool-"
 :main true)
(gen-class
 :name exodus.operations.mapreduce.multijoin.mapper
 :extends "org.apache.hadoop.mapreduce.Mapper"
 :init init
 :state state
 :prefix "mapper-")
(gen-class
 :name exodus.operations.mapreduce.multijoin.reducer
 :extends "org.apache.hadoop.mapreduce.Reducer"
 :init init
 :state state
 :prefix "reducer-")



;;cache value extraction for speed using mapper setup
(defn mapper-init []
  [[] (ref {})])

(defn mapper-setup
  [this #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	stateref (.state this)
	keyinfo (map #(Integer/parseInt %) (str/re-split #"," (.get conf "multijoin.mapper.keys")))
	valorder (str/re-split #"," (.get conf "multijoin.reducer.valorder"))
	cellfile (.get conf "multijoin.mapper.input.cell" "-1")
	valinfo (mapcat #(vector %
				 (map (fn [x] (Integer/parseInt x))
				      (str/re-split #"," (.get conf (str "multijoin.mapper.extract(" % ")")))))
			valorder)
	valinfomap (apply hash-map valinfo)]
    (if (= cellfile "-1")
      (throw (new Exception "root file not found")))
    (dosync
     (alter stateref assoc :keyinfo keyinfo :allfiles (conj valorder cellfile) :valinfo valinfomap :cellfile cellfile))))

(defn mapper-map
  [this key value #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	state (deref (.state this))
	value-string (str value)
	record (vec (str/re-split #"," value-string))
	keyinfo (:keyinfo state)
	valinfo (:valinfo state)
	allfiles (:allfiles state)
	cellfile (:cellfile state)
	file (extract-filename-from-list (.toString (.getPath #^FileSplit (.getInputSplit context)))
					 allfiles)]
    (if (= file cellfile)
      (let [key (str/str-join "," (map #(nth record %) keyinfo))]
	(.write context (Text. key) (Text. (str "cell," value-string))))
      (let [splitloc (count keyinfo)
	    key (str/str-join "," (take splitloc record))
	    val (map #(nth record %) (get valinfo file))]
	(.write context (Text. key) (Text. (str file "," (str/str-join "," val))))))))

;;get file order

(defn reducer-init []
  [[] (ref {})])

(defn reducer-setup
  [this #^Reducer$Context context]
  (let [conf (.getConfiguration context)
	stateref (.state this)
	order (str/re-split #"," (.get conf "multijoin.reducer.valorder"))
	outer (.getBoolean conf "multijoin.reducer.outerjoin" false)
	nullmap (zipmap order (repeat (count order) false))
	defaults (apply hash-map
			(mapcat
			 #(vector %
				  (repeat (.getInt conf (str "multijoin.reducer.valcount(" % ")")
						   -1) nil)) order))]
    (dosync
     (alter stateref assoc :order order :nullmap nullmap :defaults defaults :outer outer))))

(defn reducer-reduce
  [this key values #^Reducer$Context context]
  ;;catch duplicate key exception
  (let [conf (.getConfiguration context)
	state (deref (.state this))
	order (:order state)
	defaults (:defaults state)
	nullmap (:nullmap state)
	outer (:outer state)
	outer false
	allrecs (iterator-seq (.iterator values))
	typed (map #(str/re-split #"," (str %) 2) allrecs)
	splitmap
	;;build up datastructure that holds cells and values for the current key
	(loop [cells []
	       value defaults
	       complete nullmap
	       typed-rec typed]
	  (if typed-rec
	    (let [x (first typed-rec)]
	      (if (= (first x) "cell")
		(recur (conj cells (second x))
		       value
		       complete
		       (next typed-rec))
		(recur cells
		       (assoc value (first x) (second x))
		       (assoc complete (first x) true)
		       (next typed-rec))))
	    {:cells cells :value value :complete complete}))
	cells (:cells splitmap)
	value (:value splitmap)
	complete (:complete splitmap)
	useval (reduce (fn [x y] (and x y)) (map #(get complete %) order))
	finalval (str/str-join "," (map #(get value %) order))]
    (if (or outer useval)
      (doseq [cell cells]
	(.write context (Text. (str cell "," finalval)) nil)))))
 

(defn tool-run
  [#^Tool this args]
  (let [conf (.getConf this)
	job 
	(doto (Job. conf "multijoin")
	  (.setMapOutputKeyClass Text)
	  (.setMapOutputValueClass Text)
	  (.setOutputKeyClass Text)
	  (.setOutputValueClass NullWritable)
	  ;;don't think I also need to setJar for Reducer
	  (.setJarByClass exodus.operations.mapreduce.multijoin.mapper)
	  (.setMapperClass exodus.operations.mapreduce.multijoin.mapper)
	  (.setReducerClass exodus.operations.mapreduce.multijoin.reducer)
	  ;;(.setNumReduceTasks 0)
	  (.setInputFormatClass TextInputFormat)
	  (.setOutputFormatClass TextOutputFormat)
	  (FileOutputFormat/setOutputPath (Path. (first args))))]
    (doseq [file (rest args)]
      (FileInputFormat/addInputPath job (Path. file)))
    (.waitForCompletion job true)
    0))
