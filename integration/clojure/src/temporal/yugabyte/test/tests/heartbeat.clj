;; Copyright © 2023 Manetu, Inc.  All rights reserved
;;
(ns temporal.yugabyte.test.tests.heartbeat
  (:require [clojure.test :refer [is]]
            [temporal.client.core :as c]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(defactivity heartbeat-activity
  [_ _]
  (if-let [details (a/get-heartbeat-details)]
    details
    (do
      (a/heartbeat :ok)
      (throw (ex-info "heartbeat details not found" {})))))

(defworkflow heartbeat-workflow
  [_]
  @(a/invoke heartbeat-activity {}))

(deftest the-test
  (testing heartbeats "Verifies that heartbeats are handled properly"
    (fn [client]
      (let [workflow (t/create-workflow client heartbeat-workflow)]
        (c/start workflow {})
        (is (-> workflow c/get-result deref (= :ok)))))))
