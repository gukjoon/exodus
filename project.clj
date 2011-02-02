(defproject exodus "0.1"
  :description "FIXME: write"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [org.apache.hadoop/hadoop-core "0.20.2-dev"]
		 [org.clojars.gukjoon/clojure-http-client "1.1.0.1"]
		 [org.clojars.gukjoon/compojure "0.3.2"]
		 [org.clojars.gukjoon/ojdbc "1.4"]
		 [org.clojars.sids/work "0.0.1-SNAPSHOT"]]
  :aot [exodus.operations.mapreduce.hashjoin
	exodus.operations.mapreduce.mergejoin
	exodus.operations.mapreduce.multijoin
	exodus.operations.mapreduce.pipeline
	exodus.operations.mapreduce.prioritize]
  )