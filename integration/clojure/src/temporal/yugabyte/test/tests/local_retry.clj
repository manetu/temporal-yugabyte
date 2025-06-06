;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.local-retry
  (:require [clojure.test :refer [is]]
            [promesa.core :as p]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a])
  (:import (io.temporal.client WorkflowFailedException)
           [io.temporal.failure TimeoutFailure ActivityFailure]
           [java.time Duration]))

(defactivity local-retry-activity
  [ctx args]
  (log/info "local-retry-activity")
  (Thread/sleep 100000000))

(defworkflow local-retry-workflow
  [args]
  (log/info "local-retry-workflow:" args)
  @(-> (a/local-invoke local-retry-activity {} (merge args {:do-not-include-args true
                                                            :start-to-close-timeout (Duration/ofMillis 500)}))
       (p/catch ActivityFailure
                :fail)))

(defn exec [client args]
  (let [workflow (t/create-workflow client local-retry-workflow)]
    (c/start workflow args)
    @(-> (c/get-result workflow)
         (p/then (constantly :fail))
         (p/catch WorkflowFailedException
                  (fn [ex]
                    (if (instance? TimeoutFailure (ex-cause ex))
                      :pass
                      :fail))))))

(deftest the-test
  (testing policydefaults
    "RetryPolicy defaults"
    (fn [client]
      (is (= :pass (exec client {})))))
  (testing maximum-attempts-test
    "Verify that setting maximum-attempts to a finite value is respected"
    (fn [client]
      (is (= :fail (exec client {:retry-options {:maximum-attempts 1}})))))
  (testing explict-unlimited-test
    "Explicit unlimited"
    (fn [client]
      (is (= :pass (exec client {:retry-options {:maximum-attempts 0}}))))))