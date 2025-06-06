;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.exception
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(defactivity exception-activity
  [ctx args]
  (log/info "exception-activity:" args)
  (throw (ex-info "test 1" {})))

(defworkflow indirect-exception-workflow
  [args]
  (log/info "indirect-exception-workflow:" args)
  @(a/invoke exception-activity args {:retry-options {:maximum-attempts 1}}))

(defworkflow direct-exception-workflow
  [args]
  (log/info "direct-exception-workflow:" args)
  (throw (ex-info "test 2" {})))

(deftest the-test
  (testing activity-exception "Verifies that we can throw exceptions indirectly from an activity"
    (fn [client]
      (let [workflow (t/create-workflow client indirect-exception-workflow)]
        (c/start workflow {})
        (is (thrown? Exception @(c/get-result workflow))))))
  (testing workflow-exception "Verifies that we can throw exceptions directly from a workflow"
    (fn [client]
      (let [workflow (t/create-workflow client direct-exception-workflow)]
        (c/start workflow {})
        (is (thrown? Exception @(c/get-result workflow)))))))
