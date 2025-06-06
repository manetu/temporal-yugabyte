;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.sleep
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow] :as w]
            [temporal.yugabyte.test.utils :as t]
            [promesa.core :as p])
  (:import [java.time Duration]))

(defworkflow sleep-workflow
  [args]
  (log/info "sleep-workflow:" args)
  (w/sleep (Duration/ofSeconds 1))
  :ok)

(def workflow-id "sleep-workflow")
(defn create [client]
  (let [wf (c/create-workflow client sleep-workflow {:task-queue t/task-queue
                                                             :workflow-id workflow-id})]
    (c/start wf nil)
    wf))

(deftest the-test
  (testing signal-timeout "Verifies that signals may timeout properly"
    (fn [client]
      (let [wf (create client)]
        (is (= @(c/get-result wf) :ok))))))

(deftest terminate-sleep
  (testing sleep-test "Verifies that sleeping workflows can be terminated"
    (fn [client]
      (let [wf (create client)]
        (c/terminate (c/create-workflow client workflow-id) "reason" {})
        (is (= :error
               @(-> (c/get-result wf)
                    (p/catch io.temporal.client.WorkflowFailedException
                      (fn [_] :error)))))))))
