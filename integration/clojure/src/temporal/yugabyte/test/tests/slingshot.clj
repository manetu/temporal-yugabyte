;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.slingshot
  (:require [clojure.test :refer [is]]
            [taoensso.timbre :as log]
            [slingshot.slingshot :refer [try+ throw+]]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.exceptions :as e]
            [temporal.yugabyte.test.utils :as t]))

(defactivity slingshot-nonretriable-activity
  [ctx {:keys [name] :as args}]
  (log/info "slingshot-nonretriable-activity:" args)
  (throw+ {:type ::test1 ::e/non-retriable? true}))

(defactivity slingshot-retriable-activity
  [ctx {:keys [name] :as args}]
  (log/info "slingshot-retriable-activity:" args)
  (throw+ {:type ::test2}))

(defworkflow slingshot-workflow
  [args]
  (log/info "slingshot-workflow:" args)
  (try+
   @(a/invoke slingshot-nonretriable-activity args)
   (catch [:type ::test1] _
     (log/info "caught stone 1")
     (try+
      @(a/invoke slingshot-retriable-activity args {:retry-options {:maximum-attempts 2}})
      (catch [:type ::test2] _
        (log/info "caught stone 2")
        (throw+ {:type ::test3}))))))

(deftest the-test
  (testing slingshot-test "Verifies that we can catch slingshot stones across activity/workflow boundaries"
    (fn [client]
      (let [workflow (t/create-workflow client slingshot-workflow)]
        (c/start workflow {})
        (is (= :ok (try+
                     @(c/get-result workflow)
                     (throw (ex-info "should not get here" {}))
                     (catch [:type ::test3] _
                       :ok))))))))
