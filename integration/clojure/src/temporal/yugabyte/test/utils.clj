;; Copyright © Manetu, Inc.  All rights reserved

(ns temporal.yugabyte.test.utils
  "Utilities common to all tests"
  (:require [taoensso.timbre :as log]
            [integrant.core :as ig]
            [temporal.client.core :as c]
            [temporal.client.worker :as worker]
            [greenlight.step :as step])
  (:import [java.time Duration]))

(def task-queue ::default)

(log/set-level! :info)

(defn create-client
  [{:keys [temporal-address] :as params}]
  (log/info "connecting to: " temporal-address)
  (c/create-client {:target temporal-address}))

(defmethod ig/init-key ::client [_ params] (create-client params))

(defn start-worker
  [{:keys [client] :as params}]
  (worker/start client {:task-queue task-queue}))

(defmethod ig/init-key ::worker [_ params] (start-worker params))
(defmethod ig/halt-key! ::worker [_ instance] (worker/stop instance))

(def components
  {::client           {}
   ::worker           {:client (ig/ref ::client)}})

(defmacro testing
  [name title f]
  (let [sname (str name)]
    `#::step{:name (symbol ~sname)
             :title ~title
             :inputs {:client (step/component ::client)}
             :test (fn [ctx#]
                     (~f (:client ctx#)))}))

(defn create-workflow
  [client workflow]
  (c/create-workflow client workflow {:task-queue task-queue :workflow-execution-timeout (Duration/ofSeconds 10) :retry-options {:maximum-attempts 1}}))
