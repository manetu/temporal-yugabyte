;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.simple
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(defactivity simple-greet-activity
  [ctx {:keys [name] :as args}]
  (log/info "greet-activity:" args)
  (str "Hi, " name))

(defworkflow simple-greeter-workflow
  [args]
  (log/info "greeter-workflow:" args)
  @(a/invoke simple-greet-activity args))

(deftest the-test
  (testing simple-test "Verifies that we can round-trip through start"
    (fn [client]
      (let [workflow (t/create-workflow client simple-greeter-workflow)]
        (c/start workflow {:name "Bob"})
        (is (= @(c/get-result workflow) "Hi, Bob"))))))
