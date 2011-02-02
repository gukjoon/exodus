(ns exodus.common.tools
  (:require [clojure.contrib.str-utils :as str])
  )

(defmacro assert-args [fnname & pairs]
  "assert-args from clojure.core. is private in clojure.core, which is totally gay."
  `(do (when-not ~(first pairs)
	 (throw (IllegalArgumentException.
		 ~(str fnname " requires " (second pairs)))))
       ~(let [more (next pairs)]
	  (when more
	    (list* `assert-args fnname more)))))


(defn zipvec
  [& components]
  "zips seqs together into a vector of vectors"
  ;;add assert for all components to be same length
  (loop [vector []
	 components (map seq components)]
    (if (reduce (fn [x y] (and x y)) components)
      (recur (conj vector (vec (map first components)))
	     (map next components))
      vector)))


(defn form-search [form search-items]
  (let [search-items (apply hash-set search-items)]
    (loop [iter form
	   cnt 0]
      (if iter
	(let [head (first iter)]
	  (if (or (list? head) (vector? head))
	    (recur (next iter) (+ cnt (form-search head search-items)))
	    (if (contains? search-items head)
	      (recur (next iter) (+ cnt 1))
	      (recur (next iter) cnt))))
	cnt))))


(defn extract-filename [filename]
  (last (filter (fn [x] (not (re-find #"\*" x)))
		(str/re-split #"/" filename))))

(defn extract-filename-from-list [filename filelist]
  (let [results (filter #(re-find (re-pattern %) filename) filelist)]
    (println results)
    (if (= (count results) 1)
      (first results)
      (throw (new Exception "multiple file matches")))))

(defmacro dotimes-unroll
  [bindings & body]
  (cond
   (= (count bindings) 0) `(do ~@body)
   (symbol? (first bindings))
   (let [i (first bindings)
	 n (second bindings)
	 rest-b (subvec bindings 2)]
     `(dotimes [~i ~n]
	(dotimes-unroll ~rest-b ~@body)))
   :else (throw (new IllegalArgumentException "dotimes-unroll only allows symbols in bindings"))))