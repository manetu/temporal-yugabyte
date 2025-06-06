;; Copyright © Manetu, Inc.  All rights reservedvault

(ns temporal.yugabyte.test.config
  (:require [environ.core :refer [env]])
  (:refer-clojure :exclude [get]))

;; Any default may be overridden with an environment variable following
;; environ.core's keyword -> envvar translation
(def default-config
  {:temporal-address "localhost:7233"})

(def int-types #{})
(def bool-types #{})

(def truthy #{"true" "1"})

(defn convert-bool [x]
  (contains? truthy x))

(defn parse
  "This function parses a key/value by starting with a default,
  factoring in any envvar overrides, and then optionally running
  a specific parser for certain key types"
  [k default-value]
  (cond-> (env k default-value)

    (contains? int-types k)
    (parse-long)

    (contains? bool-types k)
    (convert-bool)))

(defn get []
  (reduce (fn [acc [k v]] (assoc acc k (parse k v))) {} default-config))

(defn merge-components
  ([components]
   (merge-components components (get)))
  ([components config]
   (reduce (fn [acc [k v]] (assoc acc k (merge v config))) {} components)))
