(ns exodus.operations.mapreduce.mergejoin
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
 :name exodus.operations.mapreduce.mergejoin.tool
 :extends "org.apache.hadoop.conf.Configured"
 :implements ["org.apache.hadoop.util.Tool"]
 :prefix "tool-"
 :main true)
(gen-class
 :name exodus.operations.mapreduce.mergejoin.mapper
 :extends "org.apache.hadoop.mapreduce.Mapper"
 :init init
 :state state
 :prefix "mapper-")
(gen-class
 :name exodus.operations.mapreduce.mergejoin.reducer
 :extends "org.apache.hadoop.mapreduce.Reducer" 
 :prefix "reducer-")

(defn mapper-init []
  [[] (ref {})])
  
(defn mapper-setup
  [this #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	stateref (.state this)
	keyinfo (str/re-split #"," (.get conf "mergejoin.mapper.keys"))
	configfiles (str/re-split #"," (.get conf "mergejoin.mapper.input.configs"))
	cellfile (.get conf "mergejoin.mapper.input.cell")]
    (dosync
     (alter stateref assoc :cellfile cellfile :keyinfo keyinfo :allkeys (conj configfiles cellfile)))))

(defn mapper-map
  [this key value #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	state (deref (.state this))
	value-string (str value)
	record (vec (str/re-split #"," value-string))
	keyinfo (:keyinfo state)
	cellfile (:cellfile state)
	allkeys (:allkeys state)
	file (extract-filename-from-list (.toString (.getPath #^FileSplit (.getInputSplit context)))
					 allkeys)]
    (if (= file cellfile)
      (let [key (str/str-join "," (map #(nth record (Integer/parseInt %)) keyinfo))]
	(.write context (Text. key) (Text. (str "cell," value-string))))
      (let [priority (.getInt conf
			      (str "mergejoin.mapper.priority(" file ")") 0)
	    splitloc (count keyinfo)
	    key (str/str-join "," (take splitloc record))]
	(.write context (Text. key) (Text. (str "value," priority "," (str/str-join "," (drop splitloc record)))))))))

(defn reducer-reduce
  [this key values #^Reducer$Context context]
  ;;catch duplicate key exception
  (let [conf (.getConfiguration context)
	allrecs (iterator-seq (.iterator values))
	typed (map #(str/re-split #"," (str %) 2) allrecs)
	splitmap
	(let [cells (ref [])
	      value (atom nil)]
	  (doseq [x typed]
	    (if (= (first x) "cell")
	      (dosync 
	       (alter cells conj (second x)))
	      (swap! value (fn [this]
			     (let [newval (str/re-split #"," (second x) 2)
				   newval [(Integer/parseInt (first newval)) (second newval)]]
			       (if (or (not this) (> (first newval) (first this)))
				 newval
				 this))))))
	  {:cells (deref cells) :value (deref value)})
	cells (:cells splitmap)
	value (:value splitmap)]
    (if value
      (doseq [cell cells]
	(.write context (Text. (str cell "," (second value) "," (first value))) nil)))))



(defn tool-run
  [#^Tool this args]
  (let [conf (.getConf this)
	job 
	(doto (Job. conf "mergejoin")
	  (.setMapOutputKeyClass Text)
	  (.setMapOutputValueClass Text)
	  (.setOutputKeyClass Text)
	  (.setOutputValueClass NullWritable)
	  ;;don't think I also need to setJar for Reducer
	  (.setJarByClass exodus.operations.mapreduce.mergejoin.mapper)
	  (.setMapperClass exodus.operations.mapreduce.mergejoin.mapper)
	  (.setReducerClass exodus.operations.mapreduce.mergejoin.reducer)
	  ;;(.setNumReduceTasks 0)
	  (.setInputFormatClass TextInputFormat)
	  (.setOutputFormatClass TextOutputFormat)
	  (FileOutputFormat/setOutputPath (Path. (first args))))]
    (doseq [file (rest args)]
      (FileInputFormat/addInputPath job (Path. file)))
    (.waitForCompletion job true)
    0))
