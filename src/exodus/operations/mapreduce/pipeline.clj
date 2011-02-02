(ns exodus.operations.mapreduce.pipeline
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
 :name exodus.operations.mapreduce.pipeline.tool
 :extends "org.apache.hadoop.conf.Configured"
 :implements ["org.apache.hadoop.util.Tool"]
 :prefix "tool-"
 :main true)
(gen-class
 :name exodus.operations.mapreduce.pipeline.mapper
 :init init
 :state state
 :extends "org.apache.hadoop.mapreduce.Mapper"
 :prefix "mapper-")


(defn mapper-init []
  [[] (ref {})])

(defn mapper-setup
  ;;default pipeline
  [this #^Mapper$Context context]
  (let [conf (.getConfiguration context)
	stateref (.state this)
	pipelines (.get conf "pipeline-execute.mapper.pipelines")
	pipelines (if pipelines (str/re-split #"," pipelines))
	pipemap (if pipelines
		  (apply hash-map
		       (mapcat  #(list %
				       (eval (read-string (.get conf (str "pipeline-execute.mapper.pipeline(" % ")")))))
				pipelines))
		  {"default" (eval (read-string (.get conf "pipeline-execute.mapper.pipeline(default)")))})
	pipe-index (.getInt conf "pipeline-execute.mapper.pipeline-key" -1)]
    (dosync
     (alter stateref assoc :pipelines pipemap :pindex pipe-index))))


(defn mapper-map
  [this key value #^Mapper$Context context]
  (let [state (deref (.state this))
	value-string (str value)
	record (vec (str/re-split #"," value-string))
	pipe-index (:pindex state)
	pipe-key (if (>= pipe-index 0) (nth record pipe-index) "default")
	pipe-fn (get (:pipelines state) pipe-key)
	result (apply pipe-fn record)]   
    (.write context
	    (Text. (str value-string ","
				(if (or (vector? result) (list? result))
				  (str/str-join"," result)
				  result)))
	    nil)))

(defn tool-run
  [#^Tool this args]
  (let [conf (.getConf this)
	job
	(doto (Job. conf "pipeline-executor")
	  (.setOutputKeyClass Text)
	  (.setOutputValueClass NullWritable)
	  (.setJarByClass exodus.operations.mapreduce.pipeline.mapper)
	  (.setMapperClass exodus.operations.mapreduce.pipeline.mapper)
	  (.setNumReduceTasks 0)
	  (.setInputFormatClass TextInputFormat)
	  (.setOutputFormatClass TextOutputFormat)
	  (FileOutputFormat/setOutputPath (Path. (first args))))]
    (doseq [inputfile (rest args)]
      (FileInputFormat/addInputPath job (Path. inputfile)))
    (.waitForCompletion job true)
    0))