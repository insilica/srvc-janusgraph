(ns srvc.janusgraph
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.string :as str])
  (:import (org.janusgraph.core JanusGraphFactory Multiplicity)))

(defn create-schema! [graph]
  (let [mgmt (.openManagement graph)]
    (-> mgmt (.makePropertyKey "srvcData") (.dataType Object) .make)
    (-> mgmt (.makePropertyKey "srvcExtra") (.dataType Object) .make)
    (-> mgmt (.makePropertyKey "srvcHash") (.dataType String) .make)
    (-> mgmt (.makePropertyKey "srvcType") (.dataType String) .make)
    (-> mgmt (.makePropertyKey "srvcUri") (.dataType String) .make)
    (-> mgmt (.makeEdgeLabel "srvcLabelAnswerEvent") (.multiplicity Multiplicity/ONE2MANY) .make)
    (-> mgmt (.makeEdgeLabel "srvcLabelAnswerLabel") (.multiplicity Multiplicity/ONE2MANY) .make)
    (.commit mgmt)))

(defn clj->java [x]
  (cond
    (map? x) (let [m (java.util.HashMap.)]
               (doseq [[k v] x]
                 (.put m (clj->java k) (clj->java v)))
               m)
    (seq? x) (let [l (java.util.ArrayList.)]
               (doseq [y l]
                 (.add l (clj->java y)))
               l)
    :else x))

(defn add-event! [graph {:keys [data hash type uri] :as event}]
  (let [extra (dissoc event :data :hash :type :uri)
        vtx (.addVertex graph hash)]
    (doto vtx
      (.property "srvcHash" hash)
      (.property "srvcType" type))
    (when data (.property vtx "srvcData" (clj->java data)))
    (when (seq extra) (.property vtx "srvcExtra" (clj->java extra)))
    (when uri (.property vtx "srvcUri" uri))
    (when (= type "label-answer")
      (.property vtx "srvcLabelAnswerEvent" (:event data))
      (.property vtx "srvcLabelAnswerLabel" (:label data)))
    (-> graph .tx .commit)))

(defn vertex->event [vtx]
  (let [{:strs [srvcData srvcExtra srvcHash srvcType srvcUri]}
        (-> vtx
            (.properties (into-array ["srvcData" "srvcExtra" "srvcHash" "srvcType" "srvcUri"]))
            iterator-seq
            (->> (map #(do [(-> % .propertyKey .name) (.value %)]))
                 (into {})))]
    (cond->
     (assoc srvcExtra :hash srvcHash :type srvcType)
      srvcData (assoc :data srvcData)
      srvcUri (assoc :uri srvcUri))))

(comment
  (do
    (def graph (JanusGraphFactory/open "inmemory:"))
    (create-schema! graph)
    (add-event! graph {:data {:title "The Beehive Theory"}
                       :hash "QmZp5xnczbBDvAd2ma88Q2bRFkJiKeqxQt9iN6DHc527iR"
                       :type "document"
                       :uri "https://pubmed.ncbi.nlm.nih.gov/16999303/"}))
  (-> graph .openManagement .printSchema println)
  (-> graph .traversal
      (.V (into-array []))
      (.has "srvcType" "document")
      iterator-seq
      (->> (map vertex->event))))

(defn get-label-events [graph]
  (-> graph .traversal
      (.V (into-array []))
      (.has "srvcType" "label")
      iterator-seq
      (->> (map vertex->event))))

(defn get-answers-following-labels [graph label-hashes]
  (-> graph .traversal
      (.V (into-array []))
      (.has "srvcType" "label-answer")
      iterator-seq
      (->> (map vertex->event)
           (filter #(contains? label-hashes (-> % :data :event))))))

(defn get-document-events [graph]
  (-> graph .traversal
      (.V (into-array []))
      (.has "srvcType" "document")
      iterator-seq
      (->> (map vertex->event))))

(defn get-answers-following-docs [graph doc-hashes]
  (-> graph .traversal
      (.V (into-array []))
      (.has "srvcType" "label-answer")
      iterator-seq
      (->> (map vertex->event)
           (filter #(contains? doc-hashes (-> % :data :event))))))

(defn get-output-socket []
  (let [[host port] (str/split (System/getenv "SR_OUTPUT") #"\:")]
    (java.net.Socket. host (Integer/parseInt port))))

(defn get-output-writer [socket]
  (->> socket
       .getOutputStream
       java.io.PrintWriter.))

(defn generator [db events]
  (with-open [socket (get-output-socket)
              writer (get-output-writer socket)]
    (doseq [event events]
      (json/write event writer)
      (.write writer "\n"))
    (let [graph (JanusGraphFactory/open (str "berkeleyje:" db))
          labels (get-label-events graph)
          following-labels (->> (map :hash labels)
                                set
                                (get-answers-following-labels graph))
          docs (get-document-events graph)
          following-docs (->> (map :hash docs)
                              set
                              (get-answers-following-docs graph))]
      (doseq [event (concat labels following-labels docs following-docs)]
        (json/write event writer)
        (.write writer "\n")))))

(defn sink [db events]
  (with-open [socket (get-output-socket)
              writer (get-output-writer socket)]
    (let [graph (JanusGraphFactory/open (str "berkeleyje:" db))]
      (try
        (create-schema! graph)
        ;; schema already created
        (catch org.janusgraph.core.SchemaViolationException _))
      (doseq [event events]
        (add-event! graph event)
        (json/write event writer)
        (.write writer "\n")))))

(defn -main [cmd db]
  (let [[host port] (str/split (System/getenv "SR_INPUT") #"\:")
        in-socket (when (seq host) (java.net.Socket. host (Integer/parseInt port)))
        events (some->> in-socket
                        .getInputStream
                        java.io.InputStreamReader.
                        java.io.BufferedReader.
                        line-seq
                        (keep #(when-not (str/blank? %)
                                 (json/read-str % :key-fn keyword))))]
    (case cmd
      "generator" (generator db events)
      "sink" (sink db events))
    (System/exit 0)))
