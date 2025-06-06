;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.client-signal
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :refer [>!] :as c]
            [temporal.signals :refer [<!] :as s]
            [temporal.workflow :refer [defworkflow]]
            [temporal.yugabyte.test.utils :as t]))

(def signal-name ::signal)

(defn lazy-signals [signals]
  (lazy-seq (when-let [m (<! signals signal-name)]
              (cons m (lazy-signals signals)))))

(defworkflow client-signal-workflow
  [{:keys [nr] :as args}]
  (log/info "test-workflow:" args)
  (let [signals (s/create-signal-chan)]
    (doall (take nr (lazy-signals signals)))))

(def expected 3)

(deftest the-test
  (testing client-signals "Verifies that we can send signals from a client"
    (fn [client]
      (let [workflow (t/create-workflow client client-signal-workflow)]
        (c/start workflow {:nr expected})
        (dotimes [n expected]
          (>! workflow signal-name n))
        (is (-> workflow c/get-result deref count (= expected)))))))
