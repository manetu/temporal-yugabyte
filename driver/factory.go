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

package driver

import (
	ybconfig "github.com/manetu/temporal-yugabyte/driver/config"
	localgocql "github.com/manetu/temporal-yugabyte/utils/gocql"
	"sync"

	"github.com/yugabyte/gocql"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resolver"
)

type (
	MetaFactory struct {
	}
	// InstanceFactory vends datastore implementations backed by driver
	InstanceFactory struct {
		sync.RWMutex
		cfg         ybconfig.Yugabyte
		clusterName string
		logger      log.Logger
		session     localgocql.Session
	}
)

// NewFactory returns an InstanceFactory object which can be used to create data stores that are backed by yugabyte
func (f *MetaFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) p.DataStoreFactory {
	ccfg, err := ybconfig.ImportConfig(cfg)
	if err != nil {
		logger.Fatal("unable to import configuration", tag.Error(err))
	}
	session, err := localgocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return localgocql.NewYugabyteCluster(ccfg, r)
		},
		logger,
		metricsHandler,
	)
	if err != nil {
		logger.Fatal("unable to initialize driver session", tag.Error(err))
	}
	return NewFactoryFromSession(ccfg, clusterName, logger, session)
}

// NewFactoryFromSession returns an instance of a factory object from the given session.
func NewFactoryFromSession(
	cfg ybconfig.Yugabyte,
	clusterName string,
	logger log.Logger,
	session localgocql.Session,
) p.DataStoreFactory {
	return &InstanceFactory{
		cfg:         cfg,
		clusterName: clusterName,
		logger:      logger,
		session:     session,
	}
}

// NewTaskStore returns a new task store
func (f *InstanceFactory) NewTaskStore() (p.TaskStore, error) {
	return NewMatchingTaskStore(f.session, f.logger), nil
}

// NewShardStore returns a new shard store
func (f *InstanceFactory) NewShardStore() (p.ShardStore, error) {
	return NewShardStore(f.clusterName, f.session, f.logger), nil
}

// NewMetadataStore returns a metadata store
func (f *InstanceFactory) NewMetadataStore() (p.MetadataStore, error) {
	return NewMetadataStore(f.clusterName, f.session, f.logger)
}

// NewClusterMetadataStore returns a metadata store
func (f *InstanceFactory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return NewClusterMetadataStore(f.session, f.logger)
}

// NewExecutionStore returns a new ExecutionStore.
func (f *InstanceFactory) NewExecutionStore() (p.ExecutionStore, error) {
	return NewExecutionStore(f.session), nil
}

// NewQueue returns a new queue backed by driver
func (f *InstanceFactory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return NewQueueStore(queueType, f.session, f.logger)
}

// NewQueueV2 returns a new data-access object for queues and messages stored in driver . It will never return an
// error.
func (f *InstanceFactory) NewQueueV2() (p.QueueV2, error) {
	return NewQueueV2Store(f.session, f.logger), nil
}

// NewNexusEndpointStore returns a new NexusEndpointStore
func (f *InstanceFactory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return NewNexusEndpointStore(f.session, f.logger), nil
}

// Close closes the factory
func (f *InstanceFactory) Close() {
	f.Lock()
	defer f.Unlock()
	f.session.Close()
}
