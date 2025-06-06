;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.sequence
  (:require [clojure.test :refer [is]]
            [promesa.core :as p]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]
            [temporal.promise :as pt]))

(defactivity sequence-activity
  [ctx args]
  (log/info "greet-activity:" args)
  (str "Hi, " args))

(defworkflow sequence-workflow
  [_]
  @(-> (pt/resolved true)
       (p/then (fn [_]
                 (a/invoke sequence-activity "Bob")))
       (p/then (fn [_]
                 (a/invoke sequence-activity "Charlie")))
       (p/then (constantly :ok))))

(deftest the-test
  (testing promise-chain-test "Verifies that we can process a promise-chain in sequence"
    (fn [client]
      (let [workflow (t/create-workflow client sequence-workflow)]
        (c/start workflow nil)
        (is (= @(c/get-result workflow) :ok))))))
