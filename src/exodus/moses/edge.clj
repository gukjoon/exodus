(ns exodus.moses.edge
  (:require [exodus.common.hdfs :as hdfs]
	    [clojure.contrib.str-utils :as str]
	    [clojure.contrib.duck-streams :as duck]))


(defn- add-file [this file]
  "adds a file to be handled by shared lock"
  (dosync
   (send (.file-map this) (fn [state] (assoc state file 0)))))

(defn- remove-file [this file]
  "sends a message to file-map agent to remove file
   waits for transaction to finish and checks if file was removed
   we check after because file removal is lasting/permanent"
  (dosync
   (send (.file-map this) (fn [state] (if (= (get state file) 0)
				(dissoc state file)
				state))))
  (await (.file-map this))
  (not (contains? (deref (.file-map this) file))))

(defn- take-lock [this file]
  "sends a message to agent to secure a lock for file
   waits for transaction to finish and checks if file was locked
   we check after because locking should be lasting"
  (dosync
   (send (.file-map this) (fn [state] (if (contains? state file)
				(assoc state file (+ (get state file) 1))
				state))))
  (await (.file-map this))
  (get (deref (.file-map this)) file))




(defn- get-toa-from-filename [basename filename]
  (Long/parseLong
   (second (first (re-seq (re-pattern (str basename "-([0-9]*).data"))
			  (last (str/re-split #"/" filename)))))))


(defn- get-toa-file [this toa]
  "gets latest file for toa"
  (let [basename (.name this)
	filtered-files
	(filter #(< (second %) toa)
		(map (fn [x] [x (get-toa-from-filename basename x)]) (keys (deref (.file-map this)))))]
    (if (> (count filtered-files) 0)
      (first (apply max-key second filtered-files)))))





(defprotocol edge
  (release-lock [this file] "releases lock for file")
  (take-latest [this]   "takes a lock out on latest file and returns uri")
  (clean-files [this] "deletes expired files")
  (transform [this name] "transform filename")
  (get-filename [this toa] "generates a filename for an edge given toa")
  (with-out-uri [this toa body] "executes body with an output uri as input. returns uri."))


(deftype static-edge [uri]
  edge
  (release-lock [this file])
  (take-latest [this] uri)
  (clean-files [this])
  (transform [this name] name)
  (get-filename [this toa])
  (with-out-uri [this toa body]))




(deftype exodus-edge [baseuri name expiration file-map transform]
  edge
  (release-lock [this file]
		(dosync
		 (send (.file-map this) (fn [state] (if (and (contains? state file) (> (get state file) 0))
						      (assoc state file (- (get state file) 1))
						      state)))))
  (take-latest [this]
	       (let [now (.getTime (new java.util.Date))
		     file (get-toa-file this now)]
		 (if file
		   (if (> (take-lock this file) 0)
		     file nil))))
  (transform [this name]
	     (if name
	       (str name transform)))
  (get-filename [this toa] (str baseuri "/" name "-" toa ".data"))
  (clean-files [this]
	       ;;filters file list to get expired files
	       ;;then filters by remove-file return value
	       ;;then deletes those that returned true
	       (let [now (.getTime (new java.util.Date))
		     filtered
		     (filter #(remove-file this %)
			     (filter #(< (second %) (- now expiration))
				     (map (fn [x] [x (get-toa-from-filename this x)]) (keys (deref file-map)))))]
		 (doseq [file filtered]
		   (hdfs/delete-recursive file))
		 filtered))
  (with-out-uri [this toa body]
    (let [uri (get-filename this toa)]
      (try
	(if (body uri)
	  (add-file this uri)
	  nil)
	(catch Exception e
	  (throw e))))))

(defn take-latest-all [edge-vec]
  (map take-latest edge-vec))

(defn release-all [edge-vec]
  (doseq [x edge-vec]
    (release-lock (first x) (second x))))

(defmacro with-latest [edges & body]
  "edges => [name edges] | [name edgevector]"
  #_(assert-args with-edges
	       (vector? edges) "a vector for edge bindings"
	       (even? (count edges)) "an even number of forms in edge binding")
  (cond
   (= (count edges) 0) `(do ~@body)
   (symbol? (edges 0))
   `(if (vector? ~(edges 1))
      (let [raw# (map take-latest ~(edges 1))
	    ~(edges 0) (reverse (map (fn [x#] (transform (first x#) (second x#))) (zipmap ~(edges 1) raw#)))]
	(try
	  (with-latest ~(subvec edges 2) ~@body)
	  (finally
	   (release-all (vec (zipmap ~(edges 1) raw#)))
	   #_(doseq [x# (vec (zipmap ~(edges 1) ~(edges 0)))]
	     (release-lock (first x#) (second x#))))))
      (let [raw# (take-latest ~(edges 1))
	    ~(edges 0) (transform ~(edges 1) raw#)]
	(try
	  (with-latest ~(subvec edges 2) ~@body)
	  (finally
	   (release-lock ~(edges 1) raw#)))))
   :else (throw (new IllegalArgumentException "with-latest only allows symbols in bindings"))))