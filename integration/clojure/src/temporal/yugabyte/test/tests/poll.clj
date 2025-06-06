;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.poll
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.signals :as s]
            [temporal.workflow :refer [defworkflow]]
            [temporal.yugabyte.test.utils :as t]))

(def signal-name ::signal)

(defn lazy-signals [signals]
  (lazy-seq (when-let [m (s/poll signals signal-name)]
              (cons m (lazy-signals signals)))))

(defworkflow poll-workflow
  [_]
  (log/info "test-workflow:")
  (doall (lazy-signals (s/create-signal-chan))))

(deftest the-test
  (testing poll-nil "Verifies that poll exits with nil when there are no signals"
    (fn [client]
      (let [workflow (t/create-workflow client poll-workflow)]
        (c/start workflow {})
        (is (-> workflow c/get-result deref count (= 0))))))
  (testing poll-rx "Verifies that we receive signals with poll correctly"
    (fn [client]
      (let [workflow (t/create-workflow client poll-workflow)]
        (c/signal-with-start workflow signal-name {:foo "bar"} {})
        (is (-> workflow c/get-result deref first (get :foo) (= "bar")))))))
