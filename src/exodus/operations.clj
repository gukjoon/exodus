(ns exodus.operations
  (:require [clojure.contrib.str-utils :as str]
	    [clojure.contrib.duck-streams :as duck]
	    [exodus.common.hdfs :as hdfs]
	    [clojure-http.client :as http]
	    [clojure.contrib.sql :as sql])
  (:use exodus.common.tools)
  )

;;OPERATIONS MUST RETURN FALSE UPON FAILURE AND TRUE ON SUCCESS

(defn gen-fake [keys static-val outdir]
  (let [k (map (fn [x] (gensym)) keys)
	invec (vec (mapcat (fn [x] x) (zipmap k keys)))]
    (duck/with-out-writer (hdfs/get-file-output-stream outdir)
      (eval
       `(dotimes-unroll ~invec
			(.println *out* (str (str/str-join #"," ~(conj k 'list)) "," ~(str/str-join "," static-val))))))
    true))

				

(defn http-fetch [uri outdir skip-first]
  (duck/with-out-writer (hdfs/get-file-output-stream outdir)
    (doseq [x (if skip-first
		(duck/read-lines (:body-stream (http/stream-request uri)))
		(rest (duck/read-lines (:body-stream (http/stream-request uri)))))]
      (.println *out* x)))
  true)

(defn db-fetch [db query outdir]
  (let [stream (hdfs/get-file-output-stream outdir)]
    (duck/with-out-writer stream
      (sql/with-connection db
	(sql/with-query-results rs
	  [query]
	  (doseq [r rs]
	    (.println *out* (str/str-join "," (vals r)))))))
    true))


(defn hashjoin [rootinput configinputmap output rootdomain prioritize]
  "rootinput => hdfs uri
   configinputmap => [[uri joinkeys priority]...]
   output => hdfs uri
   rootdomain => {:field index(0-based)}
   prioritize => whether or not to add priority values
   Runs a mapreduce hash join operation using rootinput as the base file, applying values from
   configinputmap"
  (if (and rootinput (reduce (fn [x y] (and x y)) (map first configinputmap)))
    (= 0
       (let [conf (new org.apache.hadoop.conf.Configuration)]
	 (if prioritize
	   (.setBoolean conf "hashjoin.mapper.prioritize" true)
	   (.setBoolean conf "hashjoin.mapper.prioritize" false))
	 (.set conf "hashjoin.mapper.allkeys" (str/str-join ","
						       (conj (map #(extract-filename (first %))
								  configinputmap)
							     (extract-filename rootinput))))
	 (doseq [[filename filekeys filepriority] configinputmap]
	   (let [filename-ext (extract-filename filename)]
	     ;;add to dcache
	     (org.apache.hadoop.filecache.DistributedCache/addCacheFile
	      (new java.net.URI filename) conf)
	     ;;set keys and name
	     (.set conf (str "hashjoin.mapper.keys(" filename-ext ")")
		   (str/str-join "," (map #(get rootdomain %) filekeys)))
	     (if (> filepriority 0)
	       (if prioritize
		 (.setInt conf (str "hashjoin.mapper.priority(" filename-ext ")") filepriority))
	       (.setBoolean conf (str "hashjoin.mapper.filter(" filename-ext ")") true))))
	 (org.apache.hadoop.util.ToolRunner/run
	  conf
	  (new exodus.operations.mapreduce.hashjoin.tool)
	  (into-array String [rootinput output]))))
    false))

;;TODO: need to handle wildcarded inputs
(defn mergejoin [rootinput configinputmap output rootdomain keys]
  "rootinput => hdfs uri
   configinputmap => [[uri priority]...]
   output => hdfs uri
   rootdomain => {:field index(0-based)}
   keys => keys to join on
   Runs a mapreduce merge join operation using rootinput as the base file, applying values from
   configinputmap joined on keys."
  (if (and rootinput (reduce (fn [x y] (and x y)) (map first configinputmap)))
    (= 0
       (let [rootfile (extract-filename rootinput)
	     conf
	     (doto (new org.apache.hadoop.conf.Configuration)
	       (.set "mergejoin.mapper.input.cell" rootfile)
	       (.set "mergejoin.mapper.input.configs"
		     (str/str-join "," (map (fn [x] (extract-filename (first x)))
					    configinputmap)))
	       (.set "mergejoin.mapper.keys" (str/str-join "," (map #(get rootdomain %) keys))))]
	 (doseq [fileconf configinputmap]
	   (let [[filename filepriority] fileconf
		 filename (extract-filename filename)]
	     (.setInt conf (str "mergejoin.mapper.priority(" filename ")") filepriority)))
	 (org.apache.hadoop.util.ToolRunner/run
	  conf
	  (new exodus.operations.mapreduce.mergejoin.tool)
	  (into-array String (conj (apply list (conj (map #(first %) configinputmap) rootinput)) output)))))
    false))

(defn multijoin [rootinput configinputmap output rootdomain keys outerjoin]
  "rootinput => hdfs uri
   configinputmap => [[uri [value...] domain-map]...]
   output => hdfs uri
   rootdomain => {:field index(0-based)}
   keys => keys to join on
   Runs a mapreduce multiple merge join operation using rootinput as the base file, applying values from
   configinputmap joined on keys. Each config input is considered a separate table."
  (if (and rootinput (reduce (fn [x y] (and x y)) (map first configinputmap)))
    (= 0
       (let [rootfile (extract-filename rootinput)
	     conf
	     (doto (new org.apache.hadoop.conf.Configuration)
	       (.setBoolean "multijoin.reducer.outerjoin" outerjoin)
	       (.set "multijoin.mapper.input.cell" rootfile)
	       (.set "multijoin.mapper.keys" (str/str-join "," (map #(get rootdomain %) keys)))
	       (.set "multijoin.reducer.valorder"
		     (str/str-join "," (map (fn [x] (extract-filename (first x)))
					    configinputmap))))]
	 (doseq [fileconf configinputmap]
	   (let [[filename filevals filedom] fileconf
		 filename (extract-filename filename)]
	     (.set conf (str "multijoin.mapper.extract(" filename ")")
		   (str/str-join "," (map #(get filedom %) filevals)))
	     (.setInt conf (str "multijoin.reducer.valcount(" filename ")")
		   (count filevals))))
	 (org.apache.hadoop.util.ToolRunner/run
	  conf
	  (new exodus.operations.mapreduce.multijoin.tool)
	  (into-array String (conj (apply list (conj (map #(first %) configinputmap) rootinput)) output)))))
    false))

(defn prioritize [inputs output configdomain keys values]
  "inputs => [uri...]
   output => hdfs uri
   configdomain => {:field index(0-based)}; must contain a :priority key
   keys => keys to consider
   values => values to output
   Runs a mapreduce prioritization operation using inputs. configdomain must contain a :priority key
   and should be uniform across all inputs. Picks highest priority value for each key. Outputs keys and values."
  (if (reduce (fn [x y] (and x y)) inputs)
    (= 0
       (let [conf
	     (doto (new org.apache.hadoop.conf.Configuration)
	       (.set "prioritizer.mapper.keys" (str/str-join "," (map #(get configdomain %) keys)))
	       (.set "prioritizer.reducer.values" (str/str-join "," (map #(get configdomain %) values)))
	       (.setInt "prioritizer.reducer.prioritykey" (:priority configdomain)))]
	 (org.apache.hadoop.util.ToolRunner/run
	  conf
	  (new exodus.operations.mapreduce.prioritize.tool)
	  (into-array String (cons output inputs)))))
    false))
  
  
(defn pipeline-execute [inputs output pipelines domain]
  "input => hdfs uri
   pipelines => {str-name fn-str} or fn-str
   domain => {:field index(0-based)}; must contain a :pipeline key if pipelines is a map"
  (if (reduce (fn [x y] (and x y)) inputs)
    (= 0
       (let [conf (new org.apache.hadoop.conf.Configuration)]
	 (if (map? pipelines)
	   (do
	     (.set conf "pipeline-execute.mapper.pipelines" (str/str-join "," (keys pipelines)))
	     (.setInt conf "pipeline-execute.mapper.pipeline-key" (get domain :pipeline))
	     (doseq [pipeconf pipelines]
	       (let [pipename (first pipeconf)
		     pipefn (second pipeconf)]
		 (.set conf (str "pipeline-execute.mapper.pipeline(" pipename ")") pipefn))))
	   (.set conf "pipeline-execute.mapper.pipeline(default)" pipelines))
	 (org.apache.hadoop.util.ToolRunner/run
	  conf
	  (new exodus.operations.mapreduce.pipeline.tool)
	  (into-array String (cons output inputs)))))
    false))
	  