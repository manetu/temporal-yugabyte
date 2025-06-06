;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.workflow-signal
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.signals :refer [<! >!] :as s]
            [temporal.workflow :refer [defworkflow]]
            [temporal.yugabyte.test.utils :as t]))

(def signal-name ::signal)

(defworkflow wfsignal-primary-workflow
  [args]
  (log/info "primary-workflow:" args)
  (let [signals (s/create-signal-chan)]
    (<! signals signal-name)))

(defworkflow wfsignal-secondary-workflow
  [{:keys [workflow-id msg]}]
  (log/info "secondary-workflow:" msg)
  (>! workflow-id signal-name msg))

(deftest the-test
  (testing signal-test "Verifies that we can send signals from a workflow"
    (fn [client]
      (let [p-wf (c/create-workflow client wfsignal-primary-workflow {:task-queue t/task-queue :workflow-id "primary"})
            s-wf (t/create-workflow client wfsignal-secondary-workflow)]
        (c/start p-wf nil)
        (c/start s-wf {:workflow-id "primary" :msg "Hi, Bob"})
        (is (= @(c/get-result p-wf) "Hi, Bob"))))))
