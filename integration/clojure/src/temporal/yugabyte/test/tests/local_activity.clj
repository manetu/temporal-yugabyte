;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.local-activity
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(defactivity local-greet-activity
  [ctx {:keys [name] :as args}]
  (log/info "greet-activity:" args)
  (str "Hi, " name))

(defworkflow local-greeter-workflow
  [args]
  (log/info "greeter-workflow:" args)
  @(a/local-invoke local-greet-activity args {:do-not-include-args true}))

(deftest the-test
  (testing local-activity "Verifies that we can invoke a local activity"
    (fn [client]
      (let [workflow (t/create-workflow client local-greeter-workflow)]
        (c/start workflow {:name "Bob"})
        (is (= @(c/get-result workflow) "Hi, Bob"))))))
