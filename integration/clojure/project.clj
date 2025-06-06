(defproject io.github.manetu/temporal-yugabyte-integration-test "0.0.1-SNAPSHOT"
  :description "An integration test for temporal-yugabyte"
  :url "https://github.com/manetu/temporal-yugabyte"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"
            :year 2025
            :key "apache-2.0"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [io.github.manetu/temporal-sdk "1.3.0"]
                 [integrant "0.13.1"]
                 [amperity/greenlight "0.7.2"]]

  :main ^:skip-aot temporal.yugabyte.test.main
  :repl-options {:init-ns user}

  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "1.5.0"]]}
             :uberjar {:aot                :all
                       :omit-source        true
                       :target-path        "target/uberjar"
                       :uberjar-name       "clojure-integration-test.jar"
                       :uberjar-exclusions [#".*manetu.*\.clj[xcs]*"]
                       :jvm-opts           ["-Dclojure.compiler.direct-linking=true"]}})
