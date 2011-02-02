(ns exodus.operations.mapreduce.prioritize
  (:require [clojure.contrib.duck-streams :as duck]
	    [clojure.contrib.str-utils :as str])
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
 :name exodus.operations.mapreduce.prioritize.tool
 :extends "org.apache.hadoop.conf.Configured"
 :implements ["org.apache.hadoop.util.Tool"]
 :prefix "tool-"
 :main true)
(gen-class
 :name exodus.operations.mapreduce.prioritize.mapper
 :init init
 :state state
 :extends "org.apache.hadoop.mapreduce.Mapper"
 :prefix "mapper-")
(gen-class
 :name exodus.operations.mapreduce.prioritize.reducer
 :init init
 :state state
 :extends "org.apache.hadoop.mapreduce.Reducer"
 :prefix "reducer-")


(defn mapper-init []
  [[] (ref {})])

(defn mapper-setup
  [this #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	stateref (.state this)
	keyinfo (map #(Integer/parseInt %) (str/re-split #"," (.get conf "prioritizer.mapper.keys")))]
    (dosync
     (alter stateref assoc :keyinfo keyinfo))))


(defn mapper-map
  [this key value #^Mapper$Context context]
  (let [state (deref (.state this))
	value-string (str value)
	record (vec (str/re-split #"," value-string))
	keyinfo (:keyinfo state)]
    (let [key (str/str-join "," (map #(nth record %) keyinfo))]
      (.write context (Text. key) (Text. value-string)))))




(defn reducer-init []
  [[] (ref {})])

(defn reducer-setup
  [this #^Reducer$Context context]
  (let [conf (.getConfiguration context)
	stateref (.state this)
	keyinfo (map #(Integer/parseInt %) (str/re-split #"," (.get conf "prioritizer.mapper.keys")))
	valinfo (map #(Integer/parseInt %) (str/re-split #"," (.get conf "prioritizer.reducer.values")))]
    (dosync
     (alter stateref assoc :keyinfo keyinfo :valinfo valinfo))))


(defn reducer-reduce
  [this key values #^Reducer$Context context]
  ;;catch duplicate key exception
  (let [state (deref (.state this))
	conf (.getConfiguration context)
	allrecs (iterator-seq (.iterator values))
	keyinfo (:keyinfo state)
	valinfo (:valinfo state)
	;; check if this is cool
	prioritykey (.getInt conf "prioritizer.reducer.prioritykey" -1)]
	(if (< prioritykey 0)
	  (throw (new Exception "no priority key defined"))
	  (let [prioritized (apply hash-map (mapcat #(let [record (str/re-split #"," (str %))]
						 ;;priority should be unique
						 [(Integer/parseInt (nth record prioritykey)) record]) allrecs))
		winner (get prioritized (apply max (keys prioritized)))]
	    (.write context (Text.
			     (str (str/str-join "," (map #(nth winner %) keyinfo)) ","
				  (str/str-join "," (map #(nth winner %) valinfo))))
		    (Text. ""))))))

(defn tool-run
  [#^Tool this args]
  (let [conf (.getConf this)
	job
	(doto (Job. conf "prioritizer")
	  (.setOutputKeyClass Text)
	  (.setOutputValueClass Text)
	  ;;don't think I also need to setJar for Reducer
	  (.setJarByClass exodus.operations.mapreduce.prioritize.mapper)
	  (.setMapperClass exodus.operations.mapreduce.prioritize.mapper)
	  (.setReducerClass exodus.operations.mapreduce.prioritize.reducer)
	  ;;(.setNumReduceTasks 0)
	  (.setInputFormatClass TextInputFormat)
	  (.setOutputFormatClass TextOutputFormat)
	  (FileOutputFormat/setOutputPath (Path. (first args))))]
    (doseq [inputfile (rest args)]
      (FileInputFormat/addInputPath job (Path. inputfile)))
    (.waitForCompletion job true)
    0))
