;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.query
  (:require [clojure.test :refer [is]]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :refer [>!] :as c]
            [temporal.signals :refer [<!] :as s]
            [temporal.workflow :as w]
            [temporal.workflow :refer [defworkflow]]
            [temporal.yugabyte.test.utils :as t]))

(def signal-name ::signal)
(def query-name ::query)

(defworkflow state-query-workflow
  [args]
  (let [state (atom 0)
        signals (s/create-signal-chan)]
    (w/register-query-handler! (fn [query-type args]
                                 @state))
    (dotimes [n 3]
      (<! signals signal-name)
      (swap! state inc))
    @state))

(deftest the-test
  (testing query-test "Verifies that we can query a workflow's state"
    (fn [client]
      (let [workflow (t/create-workflow client state-query-workflow)]
        (c/start workflow {})

        (>! workflow signal-name {})
        (>! workflow signal-name {})
        (is (= 2 (c/query workflow query-name {})))

        (>! workflow signal-name {})
        (is (= 3 (c/query workflow query-name {})))

        (is (= 3 (-> workflow c/get-result deref)))
        (is (= 3 (c/query workflow query-name {})))))))
