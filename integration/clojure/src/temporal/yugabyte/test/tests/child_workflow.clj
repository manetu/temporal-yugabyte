(ns temporal.yugabyte.test.tests.child-workflow
  (:require [clojure.test :refer [is use-fixtures]]
            [taoensso.timbre :as log]
            [greenlight.test :refer [deftest]]
            [temporal.yugabyte.test.utils :refer [testing] :as t]
            [temporal.client.core :as c]
            [temporal.workflow :refer [defworkflow] :as w]
            [temporal.activity :refer [defactivity] :as a]
            [temporal.yugabyte.test.utils :as t]))

(defactivity child-greeter-activity
  [ctx {:keys [name] :as args}]
  (log/info "greet-activity:" args)
  (str "Hi, " name))

(defworkflow child-workflow
  [{:keys [names] :as args}]
  (log/info "child-workflow:" args)
  (for [name names]
    @(a/invoke child-greeter-activity {:name name})))

(defworkflow parent-workflow
  [args]
  (log/info "parent-workflow:" args)
  @(w/invoke child-workflow args {:retry-options {:maximum-attempts 1} :task-queue t/task-queue}))

(deftest basic-child-workflow-test
  (testing child-workflow "Using a child workflow (with multiple activities) in a parent workflow works as expected"
    (fn [client]
      (let [workflow (t/create-workflow client parent-workflow)]
        (c/start workflow {:names ["Bob" "George" "Fred"]})
        (is (= (set @(c/get-result workflow)) #{"Hi, Bob" "Hi, George" "Hi, Fred"}))))))

(defworkflow parent-workflow-with-activities
  [args]
  (log/info "parent-workflow:" args)
  (concat
   @(w/invoke child-workflow args)
   [@(a/invoke child-greeter-activity {:name "Xavier"})]))

(deftest parent-workflow-with-mulitple-test
  (testing parent-workflow "Using a child workflow (with multiple activities) in a parent workflow (with activities) works as expected"
    (fn [client]
      (let [workflow (t/create-workflow client parent-workflow-with-activities)]
        (c/start workflow {:names ["Bob" "George" "Fred"]})
        (is (= (set @(c/get-result workflow)) #{"Hi, Bob" "Hi, George" "Hi, Fred" "Hi, Xavier"}))))))
