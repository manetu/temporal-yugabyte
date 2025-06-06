;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.activity-info
  (:require [clojure.test :refer [is]]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(defactivity getinfo-activity
  [ctx args]
  (a/get-info))

(defworkflow getinfo-workflow
  [args]
  @(a/invoke getinfo-activity args))

(deftest the-test
  (testing get-info "Verifies that we can retrieve our activity-id"
    (fn [client]
      (let [workflow (t/create-workflow client getinfo-workflow)]
        (c/start workflow {})
        (let [{:keys [activity-id]} @(c/get-result workflow)]
          (is (some? activity-id)))))))

