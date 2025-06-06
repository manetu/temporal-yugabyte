;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.signal-timeout
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :refer [>!] :as c]
            [temporal.signals :refer [<!] :as s]
            [temporal.workflow :refer [defworkflow]]
            [temporal.yugabyte.test.utils :as t])
  (:import [java.time Duration]))

(def signal-name ::signal)

(defworkflow timeout-workflow
  [args]
  (log/info "timeout-workflow:" args)
  (let [signals (s/create-signal-chan)]
    (or (<! signals signal-name (Duration/ofSeconds 1))
        :timed-out)))

(defn create [client]
  (let [wf (c/create-workflow client timeout-workflow {:task-queue t/task-queue})]
    (c/start wf nil)
    wf))

(deftest the-test
  (testing signal-timeout "Verifies that signals may timeout properly"
    (fn [client]
      (let [wf (create client)]
        (is (= @(c/get-result wf) :timed-out)))))
  (testing signal-ignored "Verifies that signals are received properly even when a timeout is requested"
    (fn [client]
      (let [wf (create client)]
        (>! wf ::signal "Hi, Bob")
        (is (= @(c/get-result wf) "Hi, Bob"))))))
