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
	"bytes"
	"io"
	"strings"
	"time"

	"github.com/manetu/temporal-yugabyte"
	localconfig "github.com/manetu/temporal-yugabyte/driver/config"
	localgocql "github.com/manetu/temporal-yugabyte/utils/gocql"

	"github.com/yugabyte/gocql"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/environment"
)

const (
	testSchemaDir = "../../schema/yugabyte/temporal"
)

// TestCluster allows executing yugabyte operations in testing.
type TestCluster struct {
	keyspace       string
	session        localgocql.Session
	cfg            config.CustomDatastoreConfig
	faultInjection *config.FaultInjection
	logger         log.Logger
}

// NewTestCluster returns a new yugabyte test cluster
func NewTestCluster(keyspace, username, password, host string, port int, schemaDir string, faultInjection *config.FaultInjection, logger log.Logger) *TestCluster {
	var result TestCluster
	result.logger = logger
	result.keyspace = keyspace
	if port == 0 {
		port = environment.GetCassandraPort()
	}
	if host == "" {
		host = environment.GetCassandraAddress()
	}
	options := map[string]any{}
	options["user"] = username
	options["password"] = password
	options["hosts"] = host
	options["port"] = port
	options["maxConns"] = 2
	options["connectionTimeout"] = 0 * time.Second * debug.TimeoutMultiplier
	options["keyspace"] = keyspace

	result.cfg = config.CustomDatastoreConfig{
		Name:    "yugabyte",
		Options: options,
	}
	result.faultInjection = faultInjection
	return &result
}

// Config returns the persistence config for connecting to this test cluster
func (s *TestCluster) Config() config.Persistence {
	cfg := s.cfg
	return config.Persistence{
		DefaultStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {CustomDataStoreConfig: &cfg, FaultInjection: s.faultInjection},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
}

func (s *TestCluster) CustomConfig() config.CustomDatastoreConfig {
	return s.cfg
}

// DatabaseName from PersistenceTestCluster interface
func (s *TestCluster) DatabaseName() string {
	return s.keyspace
}

// SetupTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) SetupTestDatabase() {
	s.CreateSession("system")
	s.CreateDatabase()
	s.CreateSession(s.DatabaseName())
	s.LoadSchema("create-schema.cql")
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	s.LoadSchema("drop-schema.cql")
	s.DropDatabase()
	s.session.Close()
}

// CreateSession from PersistenceTestCluster interface
func (s *TestCluster) CreateSession(
	keyspace string,
) {
	if s.session != nil {
		s.session.Close()
	}

	var err error
	op := func() error {
		ccfg, err := localconfig.ImportConfig(s.cfg)
		if err != nil {
			panic(err)
		}
		ccfg.Keyspace = keyspace
		ccfg.Consistency = &localconfig.YugabyteStoreConsistency{
			Default: &localconfig.YugabyteConsistencySettings{
				Consistency: "ONE",
			},
		}
		session, err := localgocql.NewSession(
			func() (*gocql.ClusterConfig, error) {
				return localgocql.NewYugabyteCluster(ccfg, resolver.NewNoopResolver())
			},
			log.NewNoopLogger(),
			metrics.NoopMetricsHandler,
		)
		if err == nil {
			s.session = session
		}
		return err
	}
	err = backoff.ThrottleRetry(
		op,
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		s.logger.Fatal("CreateSession", tag.Error(err))
	}
	s.logger.Debug("created session", tag.NewStringTag("keyspace", keyspace))
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	err := CreateYugabyteKeyspace(s.session, s.DatabaseName(), 1, true, s.logger)
	if err != nil {
		s.logger.Fatal("CreateYugabyteKeyspace", tag.Error(err))
	}
	s.logger.Debug("created database", tag.NewStringTag("database", s.DatabaseName()))
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	err := DropYugabyteKeyspace(s.session, s.DatabaseName(), s.logger)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		s.logger.Fatal("DropYugabyteKeyspace", tag.Error(err))
	}
	s.logger.Info("dropped database", tag.NewStringTag("database", s.DatabaseName()))
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(schemaFile string) {
	schema, err := temporal_yugabyte.SchemaFs.ReadFile("schema/yugabyte/temporal/" + schemaFile)
	if err != nil {
		s.logger.Fatal("LoadSchema", tag.Error(err))
	}
	reader := bytes.NewReader(schema)
	statements, err := p.LoadAndSplitQueryFromReaders([]io.Reader{reader})
	if err != nil {
		s.logger.Fatal("LoadSchema", tag.Error(err))
	}
	for _, stmt := range statements {
		if err = s.session.Query(stmt).Exec(); err != nil {
			s.logger.Fatal("LoadSchema", tag.Error(err))
		}
	}
	s.logger.Debug("loaded schema")
}

func (s *TestCluster) GetSession() localgocql.Session {
	return s.session
}
