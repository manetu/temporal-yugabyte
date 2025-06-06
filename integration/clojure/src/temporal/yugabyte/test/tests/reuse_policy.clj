;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.reuse-policy
  (:require [clojure.test :refer [is]]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow] :as w]
            [temporal.side-effect :as s]
            [temporal.exceptions :as e]
            [temporal.yugabyte.test.utils :as t])
  (:import [io.temporal.client WorkflowExecutionAlreadyStarted]))

(defworkflow reuse-workflow
  [args]
  (log/info "reuse-workflow:" args)
  (case args
    :sleep (w/await (constantly false))
    :crash (throw+ {:type ::synthetic-crash ::e/non-retriable? true})
    :exit  (s/gen-uuid)))

(defn invoke [client {:keys [policy wid args] :or {wid ::wid args :exit policy :allow-duplicate}}]
  (let [wf (c/create-workflow client reuse-workflow {:task-queue t/task-queue
                                                             :workflow-id wid
                                                             :workflow-id-reuse-policy policy})]
    (c/start wf args)
    @(c/get-result wf)))

(deftest the-test
  (testing allow-duplicate "Verifies that :allow-duplicate policy works"
    (fn [client]
      (dotimes [_ 2]
        (is (some? (invoke client {:wid ::allow-duplicate :policy :allow-duplicate}))))))
  (testing allow-duplicate-fail-only "Verifies that :allow-duplicate-failed-only policy works"
    (fn [client]
      (try+
        (invoke client {:wid ::allow-duplicate-failed-only :args :crash})
        (catch [:type ::synthetic-crash] _
          :ok))
      (is (some? (invoke client {:wid ::allow-duplicate-failed-only :policy :allow-duplicate-failed-only})))))
  (testing reject-duplicate "Verifies that :reject-duplicate policy works"
    (fn [client]
      (let [result (invoke client {:wid ::reject-duplicate :policy :reject-duplicate})]
        (is (thrown? WorkflowExecutionAlreadyStarted (invoke client {:wid ::reject-duplicate :policy :reject-duplicate})))
        (let [wf2 (c/create-workflow client ::reject-duplicate)]
          (is (= result @(c/get-result wf2)))))))
  (testing terminate-if-running "Verifies that :terminate-if-running policy works"
    (fn [client]
      (let [wf (c/create-workflow client reuse-workflow {:task-queue t/task-queue :workflow-id ::terminate-if-running})]
        (c/start wf :sleep)
        (is (some? (invoke client {:wid ::terminate-if-running :policy :terminate-if-running}))))))
  (testing bogus-policy-check "Verifies that a bogus reuse policy throws"
    (fn [client]
      (is (thrown? IllegalStateException (invoke client {:wid ::bogus :policy :bogus}))))))
