;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.side-effect
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.side-effect :as side-effect]
            [temporal.yugabyte.test.utils :as t])
  (:import [java.time Instant]))

(defworkflow side-effect-workflow
  [args]
  (log/info "workflow:" args)
  (side-effect/now))

(deftest the-test
  (testing side-effect-test "Verifies that we can round-trip through start"
    (fn [client]
      (let [workflow (t/create-workflow client side-effect-workflow)]
        (c/start workflow {})
        (is (instance? Instant @(c/get-result workflow)))))))