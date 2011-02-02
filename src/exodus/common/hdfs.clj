(ns exodus.common.hdfs
  (:require [clojure.contrib.duck-streams :as duck]
	    [clojure.contrib.str-utils :as str])
  (:import [org.apache.hadoop.fs Path]
	   [org.apache.hadoop.conf Configuration])
  )

(defn get-file-output-stream [uri]
  (let [path (new Path uri)
	fs (.getFileSystem path (new Configuration))
	stream (.create fs path true)]
    stream))

(defn get-file-input-stream [uri]
  (let [path (new Path uri)
	fs (.getFileSystem path (new Configuration))
	stream (.open fs path)]
    stream))

(defn get-file-input-seq [uri]
  (let [path (new Path uri)
	fs (.getFileSystem path (new Configuration))
	streamseq (map #(.open fs (.getPath %)) (filter #(not (.isDir %)) (.globStatus fs path)))]
    streamseq))

(defn get-file-uri-vec [uri priority]
  (let [path (new Path uri)
	fs (.getFileSystem path (new Configuration))]
    (vec (map (fn [x] [(str (.getPath x)) priority]) (.globStatus fs path)))))

(defn delete-recursive [uri]
  (let [path (new Path uri)
	fs (.getFileSystem path (new Configuration))]
    (.delete fs path true)))

#_(defn cat-streams [uri output]
  (let [inseq (get-file-input-seq uri)]
    (duck/with-out-writer (get-file-output-stream output)
      (doseq [path inseq]
	(doseq [line (duck/read-lines path)]
	  (.println *out* line))))))