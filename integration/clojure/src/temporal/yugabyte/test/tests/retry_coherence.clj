;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.retry-coherence
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t])
  (:import [java.time Duration]))

(defactivity retry-activity
  [_ {:keys [mode]}]
  (let [{:keys [activity-id]} (a/get-info)]
    (log/info "retry-activity:" activity-id)
    (if-let [details (a/get-heartbeat-details)]
      (do
        (log/info "original activity-id:" activity-id "current activity-id:" details)
        (= activity-id details))
      (do
        (a/heartbeat activity-id)
        (case mode
          :crash (throw (ex-info "synthetic crash" {}))
          :timeout (Thread/sleep 2000))))))

(defworkflow retry-workflow
  [args]
  @(a/invoke retry-activity args {:start-to-close-timeout (Duration/ofSeconds 1)}))

(deftest the-test
  (testing stable-crash-retry "Verifies that a retriable crash has a stable activity-id"
    (fn [client]
      (let [workflow (t/create-workflow client retry-workflow)]
        (c/start workflow {:mode :crash})
        (is (-> workflow c/get-result deref true?)))))
  (testing stable-timeout-retry "Verifies that a timeout retry has a stable activity-id"
    (fn [client]
      (let [workflow (t/create-workflow client retry-workflow)]
        (c/start workflow {:mode :timeout})
        (is (-> workflow c/get-result deref true?))))))
