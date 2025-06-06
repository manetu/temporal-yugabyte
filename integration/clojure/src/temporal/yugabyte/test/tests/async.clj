;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.async
  (:require [clojure.test :refer [is use-fixtures]]
            [clojure.core.async :refer [go]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.client.core :as c]
            [temporal.yugabyte.test.utils :as t]
            [temporal.workflow :refer [defworkflow] :as w]))

(defactivity async-greet-activity
  [ctx {:keys [name] :as args}]
  (go
    (log/info "greet-activity:" args)
    (if (= name "Charlie")
      (ex-info "permission-denied" {})                      ;; we don't like Charlie
      (str "Hi, " name))))

(defworkflow async-greeter-workflow
  [args]
  (log/info "greeter-workflow:" args)
  @(a/invoke async-greet-activity args {:retry-options {:maximum-attempts 1}}))

(deftest basic-async-test
  (testing async-e2e "Verifies that we can round-trip with an async task"
    (fn [client]
      (let [workflow (t/create-workflow client async-greeter-workflow)]
        (c/start workflow {:name "Bob"})
        (is (= @(c/get-result workflow) "Hi, Bob")))))
  (testing async-errors "Verifies that we can process errors in async mode"
    (fn [client]
      (let [workflow (t/create-workflow client async-greeter-workflow)]
        (c/start workflow {:name "Charlie"})
        (is (thrown? java.util.concurrent.ExecutionException
                     @(c/get-result workflow)))))))

(defactivity async-child-activity
  [ctx {:keys [name] :as args}]
  (go
    (log/info "async-child-activity:" args)
    (if (= name "Charlie")
      (ex-info "permission-denied" {})
      (str "Hi, " name))))

(defworkflow async-child-workflow
  [{:keys [name] :as args}]
  (log/info "async-child-workflow:" args)
  @(a/invoke async-child-activity args {:retry-options {:maximum-attempts 1}}))

(defworkflow async-parent-workflow
  [args]
  (log/info "async-parent-workflow:" args)
  @(w/invoke async-child-workflow args {:retry-options {:maximum-attempts 1} :task-queue t/task-queue}))

(deftest child-workflow-test
  (testing child-async-e2e "Verifies that we can round-trip with an async task"
    (fn [client]
      (let [workflow (t/create-workflow client async-parent-workflow)]
        (c/start workflow {:name "Bob"})
        (is (= @(c/get-result workflow) "Hi, Bob")))))
  (testing child-async-errors "Verifies that we can process errors in async mode"
    (fn [client]
      (let [workflow (t/create-workflow client async-parent-workflow)]
        (c/start workflow {:name "Charlie"})
        (is (thrown? java.util.concurrent.ExecutionException
                     @(c/get-result workflow)))))))
