(ns exodus.operations.mapreduce.hashjoin
  (:require [clojure.contrib.duck-streams :as duck]
	    [clojure.contrib.str-utils :as str])
  (:use exodus.common.tools)
  (:import (java.util StringTokenizer)
	   (java.net URI)
           (org.apache.hadoop.util Tool)
	   (org.apache.hadoop.io LongWritable Text NullWritable)
	   (org.apache.hadoop.fs Path)
	   (org.apache.hadoop.filecache DistributedCache)
	   (org.apache.hadoop.mapreduce Mapper Mapper$Context Job)
	   (org.apache.hadoop.mapreduce.lib.input TextInputFormat FileInputFormat)
	   (org.apache.hadoop.mapreduce.lib.output TextOutputFormat FileOutputFormat)))

;;gen-class directives
;;see http://clojure.org/compilation for documentation
(gen-class
 :name exodus.operations.mapreduce.hashjoin.tool
 :extends "org.apache.hadoop.conf.Configured"
 :implements ["org.apache.hadoop.util.Tool"]
 :prefix "tool-"
 :main true)
(gen-class
 :name exodus.operations.mapreduce.hashjoin.mapper
 :extends "org.apache.hadoop.mapreduce.Mapper"
 :init init
 :state state
 :prefix "mapper-")

;;constructor for mapper.
;;Reads in master list of RTB sites and Campaign/Action mapping
;;These are kept in memory for now under the assumption that they will be small
;;replace with new MR API method 'setup'

(defn- gen-hash [config n]
  ;;parse config using the first n columns as the key
  (apply hash-map
	 (mapcat 
	  #(let [split-config (str/re-split #"," % (+ n 1))]
	     (if (> (count split-config) n)
	       [(str/str-join "," (take n split-config)) (nth split-config n)]
	       (if (= (count split-config) n)
		 [(str/str-join "," (take n split-config)) true]
		 (throw (new Exception (str "invalid key selection for config " config))))))
	  (duck/read-lines config))))

(defn mapper-init []
  [[] (ref {})])


(defn mapper-setup
  [this #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	stateref (.state this)
	cachefiles (DistributedCache/getLocalCacheFiles conf)]
    (dosync
     (doseq [config cachefiles]
       (let [config-str (.toString config)
	     filename (extract-filename config-str)
	     keys (map #(Integer/parseInt %) (str/re-split #"," (.get conf (str "hashjoin.mapper.keys(" filename ")"))))
	     keycount (count keys)
	     filter (.getBoolean conf (str "hashjoin.mapper.filter(" filename ")") false)
	     priority (.getInt conf (str "hashjoin.mapper.priority(" filename ")") 0)]
	 (alter stateref assoc filename
		{:keys keys :data (gen-hash config-str keycount)
		 :filter filter :priority priority}))))))


(defn mapper-map
  [this key value #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	prioritize (.getBoolean conf "hashjoin.mapper.prioritize" false)
	state (deref (.state this))
	non-filters (filter #(not (:filter (second %))) state)
	filters (filter #(:filter (second %)) state)
	value-string (str value)
	record (vec (str/re-split #"," value-string))]
    (if (or (< (count filters) 1)
	    (reduce (fn [x y] (and x y))
		    (map (fn [filter]
			   (get (:data (second filter))
				(str/str-join "," (map #(nth record %) (:keys (second filter)))))) filters)))
      (if prioritize
	(let [maybe-value
	  (reduce (fn [x y] (if (second y) (if (> (first x) (first y)) x y) x))
		  (conj
		   (map
		    (fn [x]
		      [(:priority (second x))
		       (get (:data (second x))
			    (str/str-join "," (map #(nth record %) (:keys (second x)))))])
		    non-filters)
		   [1 nil]))]
	  (if (second maybe-value)
	    (.write context (Text. (str value-string "," (second maybe-value) "," (first maybe-value))) nil)))
	(doseq [cache non-filters]
	  (let [maybe-value
		(get (:data (second cache)) (str/str-join "," (map #(nth record %) (:keys (second cache)))))]
	    (if maybe-value
	      (.write context (Text. (str value-string "," maybe-value))) nil)))))))

(defn tool-run
  [#^Tool this args]
  (let [conf (.getConf this)]
    ;;add global static files to cache
    (doto (Job. conf "sitefilter")
      (.setOutputKeyClass Text)
      (.setOutputValueClass NullWritable)
      (.setJarByClass exodus.operations.mapreduce.hashjoin.mapper)
      (.setMapperClass exodus.operations.mapreduce.hashjoin.mapper)
      (.setNumReduceTasks 0)
      (.setInputFormatClass TextInputFormat)
      (.setOutputFormatClass TextOutputFormat)
      (FileInputFormat/setInputPaths (first args))
      (FileOutputFormat/setOutputPath (Path. (second args)))
      (.waitForCompletion true))
    0))