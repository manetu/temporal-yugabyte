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
	"context"
	"github.com/manetu/temporal-yugabyte/utils/gocql"
	"time"

	p "go.temporal.io/server/common/persistence"
)

const (
	// system_tasks 'id' names for well-known services, guaranteed not to conflict with DLQ
	systemTaskTransfer    = "70454977-3BF9-4298-AC1A-93DD68718ACB"
	systemTaskReplication = "D079A3AD-8A39-431A-A7AC-D49DC5D4964B"
	systemTaskVisibility  = "8E2B1883-5646-4306-8739-7CFEB6755EAD"
)

const (
	// Row types for table executions
	rowTypeTimerTask = iota
	// NOTE: the row type for history task is the task category ID
	// rowTypeHistoryTask
)

const (
	// Row types for table tasks
	rowTypeTask = iota
	rowTypeTaskQueue
)

const (
	taskQueueTaskID = -12345

	// ref: https://docs.datastax.com/en/dse-trblshoot/doc/troubleshooting/recoveringTtlYear2038Problem.html
	maxCassandraTTL = int64(315360000) // Cassandra max support time is 2038-01-19T03:14:06+00:00. Updated this to 10 years to support until year 2028
)

var (
	defaultDateTime            = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	defaultVisibilityTimestamp = p.UnixMilliseconds(defaultDateTime)
)

type (
	ExecutionStore struct {
		*HistoryStore
		*MutableStateStore
		*MutableStateTaskStore
	}
)

var _ p.ExecutionStore = (*ExecutionStore)(nil)

func NewExecutionStore(session gocql.Session) *ExecutionStore {
	return &ExecutionStore{
		HistoryStore:          NewHistoryStore(session),
		MutableStateStore:     NewMutableStateStore(session),
		MutableStateTaskStore: NewMutableStateTaskStore(session),
	}
}

func (d *ExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	for _, req := range request.NewWorkflowNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return nil, err
		}
	}

	return d.MutableStateStore.CreateWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	for _, req := range request.UpdateWorkflowNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.NewWorkflowNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}

	return d.MutableStateStore.UpdateWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	for _, req := range request.CurrentWorkflowEventsNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.ResetWorkflowEventsNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.NewWorkflowEventsNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}

	return d.MutableStateStore.ConflictResolveWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) GetName() string {
	return yugabytePersistenceName
}

func (d *ExecutionStore) Close() {
	if d.HistoryStore.Session != nil {
		d.HistoryStore.Session.Close()
	}
	if d.MutableStateStore.Session != nil {
		d.MutableStateStore.Session.Close()
	}
	if d.MutableStateTaskStore.Session != nil {
		d.MutableStateTaskStore.Session.Close()
	}
}
