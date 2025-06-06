;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.signal-with-start
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.signals :refer [<!] :as s]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(def signal-name ::signal)

(defactivity signal-greet-activity
  [ctx {:keys [greeting name] :as args}]
  (log/info "greet-activity:" args)
  (str greeting ", " name))

(defworkflow signal-greeter-workflow
  [args]
  (log/info "greeter-workflow:" args)
  (let [signals (s/create-signal-chan)
        m (<! signals signal-name)]
    @(a/invoke signal-greet-activity (merge args m))))

(deftest the-test
  (testing signal-with-start "Verifies that we can round-trip through signal-with-start"
    (fn [client]
      (let [workflow (t/create-workflow client signal-greeter-workflow)]
        (c/signal-with-start workflow signal-name {:name "Bob"} {:greeting "Hi"})
        (is (= @(c/get-result workflow) "Hi, Bob"))))))
