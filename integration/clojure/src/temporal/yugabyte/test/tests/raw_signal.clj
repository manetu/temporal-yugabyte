;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.raw-signal
  (:require [clojure.test :refer [is]]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :refer [>!] :as c]
            [temporal.signals :refer [<!] :as s]
            [temporal.workflow :refer [defworkflow] :as w]
            [temporal.yugabyte.test.utils :as t]))

(def signal-name ::signal)

(defworkflow raw-signal-workflow
  [args]
  (let [state (atom 0)]
    (s/register-signal-handler! (fn [signal-name args]
                                  (swap! state inc)))
    (w/await (fn [] (> @state 1)))
    @state))

(deftest the-test
  (testing raw-signals "Verifies that we can handle raw signals"
    (fn [client]
      (let [workflow (t/create-workflow client raw-signal-workflow)]
        (c/start workflow {})

        (>! workflow signal-name {})
        (>! workflow signal-name {})
        (is (= 2 @(c/get-result workflow)))))))
