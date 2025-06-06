;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.resolved-promises
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.promise :as pt]
            [temporal.yugabyte.test.utils :as t]))

(defworkflow all-workflow
  [args]
  (log/info "workflow:" args)
  @(pt/all [(pt/resolved true)])
  @(pt/race [(pt/resolved true)])
  :ok)

(deftest the-test
  (testing promise-resolution "Verifies that pt/resolved and pt/rejected are compatible with all/race"
    (fn [client]
      (let [workflow (t/create-workflow client all-workflow)]
        (c/start workflow {})
        (is (-> workflow c/get-result deref (= :ok)))))))
