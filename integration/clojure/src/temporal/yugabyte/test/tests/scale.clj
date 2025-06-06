;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.tests.scale
  (:require [clojure.test :refer [is]]
            [clojure.core.async :refer [go <!] :as async]
            [promesa.core :as p]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow]]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(defactivity scale-activity
  [ctx {:keys [id] :as args}]
  (log/info "activity:" args)
  (go
    (<! (async/timeout 1000))
    id))

(defworkflow scale-workflow
  [args]
  (log/info "workflow:" args)
  @(a/invoke scale-activity args))

(defn create [client id]
  (let [workflow (t/create-workflow client scale-workflow)]
    (c/start workflow {:id id})
    (c/get-result workflow)))

(deftest the-test
  (testing scale-test "Verifies that we can launch workflows in parallel"
    (fn [client]
      (let [nr 500]
        (time
          (is (-> @(p/all (map (partial create client) (range nr))) count (= nr))))))))
