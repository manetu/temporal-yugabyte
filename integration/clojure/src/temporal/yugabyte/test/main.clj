;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [integrant.core :as ig]
            [greenlight.runner :as runner]
            [taoensso.timbre :as log]
            [temporal.yugabyte.test.config :as config]
            [temporal.yugabyte.test.utils :as utils]
            [temporal.yugabyte.test.tests.activity-info]
            [temporal.yugabyte.test.tests.async]
            [temporal.yugabyte.test.tests.child-workflow]
            [temporal.yugabyte.test.tests.client-signal]
            [temporal.yugabyte.test.tests.concurrency]
            [temporal.yugabyte.test.tests.exception]
            [temporal.yugabyte.test.tests.heartbeat]
            [temporal.yugabyte.test.tests.local-activity]
            [temporal.yugabyte.test.tests.local-retry]
            [temporal.yugabyte.test.tests.poll]
            [temporal.yugabyte.test.tests.query]
            [temporal.yugabyte.test.tests.race]
            [temporal.yugabyte.test.tests.raw-signal]
            [temporal.yugabyte.test.tests.resolved-promises]
            [temporal.yugabyte.test.tests.retry-coherence]
            [temporal.yugabyte.test.tests.reuse-policy]
            [temporal.yugabyte.test.tests.scale]
            [temporal.yugabyte.test.tests.sequence]
            [temporal.yugabyte.test.tests.side-effect]
            [temporal.yugabyte.test.tests.signal-timeout]
            [temporal.yugabyte.test.tests.signal-with-start]
            [temporal.yugabyte.test.tests.simple]
            [temporal.yugabyte.test.tests.sleep]
            [temporal.yugabyte.test.tests.slingshot]
            [temporal.yugabyte.test.tests.uuid-test]
            [temporal.yugabyte.test.tests.workflow-signal])
  (:gen-class))

(def options
  [["-h" "--help"]
   ["-v" "--version" "Print the version and exit"]])

(defn exit [status msg & args]
  (apply println msg args)
  status)

(def code-version (System/getProperty "temporal-yugabyte-integration-test.version"))

(defn print-version [] (str "temporal-yugabyte integration tests version: v" code-version))

(defn prep-usage [msg] (->> msg flatten (string/join \newline)))

(defn usage [options-summary]
  (prep-usage [(print-version)
               ""
               "Usage: temporal-yugabyte-integration-test [options]"
               ""
               "Options:"
               options-summary]))

(extend-protocol runner/ManagedSystem
  java.util.Map
  (start-system [this] (ig/init (ig/prep (config/merge-components this))))
  (stop-system [this] (ig/halt! this)))

(defn exec
  []
  (runner/run-tests! (constantly utils/components)
                     (runner/find-tests) {}))

(defn -app
  [& args]
  (let [{{:keys [help] :as options} :options :keys [errors summary]} (parse-opts args options)]
    (cond

      help
      (exit 0 (usage summary))

      (not= errors nil)
      (exit -1 "Error: " (string/join errors))

      (:version options)
      (exit 0 (print-version))

      :else
      (try
        (if (exec)
          (exit 0 "")
          (exit -1 ""))
        (catch Exception e
          (log/error e)
          (exit -1 (ex-message e)))))))

(defn -main
  [& args]
  (System/exit (apply -app args)))
