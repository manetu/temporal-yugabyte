;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.uuid-test
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.yugabyte.test.utils :as t]
            [temporal.side-effect :refer [gen-uuid]]))

(defworkflow uuid-workflow
  [args]
  (log/info "workflow:" args)
  (gen-uuid))

(deftest the-test
  (testing uuid-test "Verifies that we can use the side-effect safe gen-uuid"
    (fn [client]
      (let [workflow (t/create-workflow client uuid-workflow)]
        (c/start workflow {})
        (let [r  @(c/get-result workflow)]
          (log/debug "r:" r)
          (is (some? r)))))))
