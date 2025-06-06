// The MIT License
//
// Copyright (c) 2025 Manetu Inc.  All rights reserved.
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package core

import (
	"fmt"
	"go.temporal.io/server/common/log/tag"
	"testing"

	"github.com/manetu/temporal-yugabyte/driver"
	localconfig "github.com/manetu/temporal-yugabyte/driver/config"
	localgocql "github.com/manetu/temporal-yugabyte/utils/gocql"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.uber.org/zap/zaptest"
)

// TODO merge the initialization with existing persistence setup
const (
	testYugabyteClusterName = "temporal_yugabyte_cluster"

	testYugabyteDatabaseNamePrefix = "test_"
	testYugabyteDatabaseNameSuffix = "temporal_persistence"
)

type (
	YugabyteTestData struct {
		Cfg     localconfig.Yugabyte
		Factory p.DataStoreFactory
		Logger  log.Logger
	}
)

func setUpYugabyteTest(t *testing.T) (YugabyteTestData, func()) {
	keyspace := testYugabyteDatabaseNamePrefix + shuffle.String(testYugabyteDatabaseNameSuffix)
	logger := log.NewZapLogger(zaptest.NewLogger(t))

	cluster := NewTestCluster(keyspace, "", "", "", 0, "", &config.FaultInjection{}, logger)
	cluster.SetupTestDatabase()

	cfg := cluster.CustomConfig()

	ccfg, err := localconfig.ImportConfig(cfg)
	if err != nil {
		panic("unable to import configuration")
	}

	metaFactory := &driver.MetaFactory{}

	factory := metaFactory.NewFactory(
		cfg,
		resolver.NewNoopResolver(),
		testYugabyteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)

	testData := YugabyteTestData{
		Cfg:     ccfg,
		Factory: factory,
		Logger:  logger,
	}

	tearDown := func() {
		factory.Close()
		cluster.TearDownTestDatabase()
	}

	return testData, tearDown
}

// NewTestBaseWithYugabyte returns a persistence test base backed by yugabyte datastore
func NewTestBaseWithYugabyte(options *persistencetests.TestBaseOptions) *persistencetests.TestBase {
	logger := log.NewTestLogger()
	testCluster := NewTestClusterForYugabyte(options, logger)
	testBase := persistencetests.NewTestBaseForCluster(testCluster, logger)
	testBase.AbstractDataStoreFactory = &driver.MetaFactory{}
	return testBase
}

func NewTestClusterForYugabyte(options *persistencetests.TestBaseOptions, logger log.Logger) *TestCluster {
	if options.DBName == "" {
		options.DBName = "test_" + persistencetests.GenerateRandomDBName(3)
	}
	testCluster := NewTestCluster(options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.SchemaDir, options.FaultInjection, logger)
	return testCluster
}

// CreateYugabyteKeyspace creates the keyspace using this session for given replica count
func CreateYugabyteKeyspace(s localgocql.Session, keyspace string, replicas int, overwrite bool, logger log.Logger) (err error) {
	// if overwrite flag is set, drop the keyspace and create a new one
	if overwrite {
		err = DropYugabyteKeyspace(s, keyspace, logger)
		if err != nil {
			logger.Error("drop keyspace error", tag.Error(err))
			return
		}
	}
	err = s.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
		'class' : 'SimpleStrategy', 'replication_factor' : %d}`, keyspace, replicas)).Exec()
	if err != nil {
		logger.Error("create keyspace error", tag.Error(err))
		return
	}
	logger.Debug("created keyspace", tag.Value(keyspace))

	return
}

// DropYugabyteKeyspace drops the given keyspace, if it exists
func DropYugabyteKeyspace(s localgocql.Session, keyspace string, logger log.Logger) (err error) {
	err = s.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	if err != nil {
		logger.Error("drop keyspace error", tag.Error(err))
		return
	}
	logger.Debug("dropped keyspace", tag.Value(keyspace))
	return
}
