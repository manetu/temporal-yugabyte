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
	"fmt"
	"github.com/manetu/temporal-yugabyte/utils/gocql"
	"time"

	p "go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

const (
	templateCreateSystemTaskQuery = `INSERT INTO system_tasks (` +
		`shard_id, id, data, encoding, task_id) ` +
		`VALUES(?, ?, ?, ?, ?) `

	templateCreateTransferTaskQuery = templateCreateSystemTaskQuery

	templateCreateReplicationTaskQuery = templateCreateSystemTaskQuery

	templateCreateVisibilityTaskQuery = templateCreateSystemTaskQuery

	templateCreateTimerTaskQuery = `INSERT INTO timers (` +
		`shard_id, type, data, encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?) `

	templateCreateHistoryTaskQuery = templateCreateTimerTaskQuery

	templateGetHistoryImmediateTasksQuery = `SELECT task_id, data, encoding ` +
		`FROM timers ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateGetHistoryScheduledTasksQuery = `SELECT visibility_ts, task_id, data, encoding ` +
		`FROM timers ` +
		`WHERE shard_id = ? ` +
		`and type = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateGetSystemTasksQuery = `SELECT task_id, data, encoding ` +
		`FROM system_tasks ` +
		`WHERE shard_id = ? ` +
		`and id = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateGetTransferTasksQuery = templateGetSystemTasksQuery

	templateGetVisibilityTasksQuery = templateGetSystemTasksQuery

	templateGetReplicationTasksQuery = templateGetSystemTasksQuery

	templateIsQueueEmptyQuery = `SELECT task_id ` +
		`FROM system_tasks ` +
		`WHERE shard_id = ? ` +
		`and id = ? ` +
		`and task_id >= ? ` +
		`limit 1`

	templateCompleteSystemTaskQuery = `DELETE FROM system_tasks ` +
		`WHERE shard_id = ? ` +
		`and id = ? ` +
		`and task_id = ? `

	templateRangeCompleteSystemTaskQuery = `DELETE FROM system_tasks ` +
		`WHERE shard_id = ? ` +
		`and id = ? ` +
		`and task_id >= ? ` +
		`and task_id < ? `

	templateCompleteTransferTaskQuery = templateCompleteSystemTaskQuery

	templateRangeCompleteTransferTaskQuery = templateRangeCompleteSystemTaskQuery

	templateCompleteVisibilityTaskQuery = templateCompleteSystemTaskQuery

	templateRangeCompleteVisibilityTaskQuery = templateRangeCompleteTransferTaskQuery

	templateCompleteReplicationTaskQuery = templateCompleteSystemTaskQuery

	templateRangeCompleteReplicationTaskQuery = templateRangeCompleteTransferTaskQuery

	templateCompleteHistoryTaskQuery = templateCompleteTimerTaskQuery

	templateRangeCompleteHistoryImmediateTasksQuery = `DELETE FROM timers ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ? `

	templateRangeCompleteHistoryScheduledTasksQuery = templateRangeCompleteTimerTaskQuery

	templateGetTimerTasksQuery = `SELECT visibility_ts, task_id, data, encoding ` +
		`FROM timers ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateCompleteTimerTaskQuery = `DELETE FROM timers ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateRangeCompleteTimerTaskQuery = `DELETE FROM timers ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ? `
)

type (
	MutableStateTaskStore struct {
		Session gocql.Session
	}
)

func NewMutableStateTaskStore(session gocql.Session) *MutableStateTaskStore {
	return &MutableStateTaskStore{
		Session: session,
	}
}

func (d *MutableStateTaskStore) AddHistoryTasks(
	ctx context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
	txn := d.Session.NewTxn().WithContext(ctx)

	if err := applyTasks(
		txn,
		request.ShardID,
		request.Tasks,
	); err != nil {
		return err
	}

	txn.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		request.RangeID,
	)

	err := txn.Exec()
	if err != nil {
		return err
	}
	//previous := make(map[string]interface{})
	//applied, iter, err := d.Session.MapExecuteBatchCAS(txn, previous)
	//if err != nil {
	//	return gocql.ConvertError("AddTasks", err)
	//}
	//defer func() {
	//	_ = iter.Close()
	//}()
	//
	//if !applied {
	//	if previousRangeID, ok := previous["range_id"].(int64); ok && previousRangeID != request.RangeID {
	//		// CreateWorkflowExecution failed because rangeID was modified
	//		return &p.ShardOwnershipLostError{
	//			ShardID: request.ShardID,
	//			Msg:     fmt.Sprintf("Failed to add tasks.  Request RangeID: %v, Actual RangeID: %v", request.RangeID, previousRangeID),
	//		}
	//	} else {
	//		return serviceerror.NewUnavailable("AddTasks operation failed: %v")
	//	}
	//}
	return nil
}

func (d *MutableStateTaskStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.getTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		return d.getTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.getVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return d.getReplicationTasks(ctx, request)
	default:
		return d.getHistoryTasks(ctx, request)
	}
}

func (d *MutableStateTaskStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.completeTransferTask(ctx, request)
	case tasks.CategoryIDTimer:
		return d.completeTimerTask(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.completeVisibilityTask(ctx, request)
	case tasks.CategoryIDReplication:
		return d.completeReplicationTask(ctx, request)
	default:
		return d.completeHistoryTask(ctx, request)
	}
}

func (d *MutableStateTaskStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.rangeCompleteTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		return d.rangeCompleteTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.rangeCompleteVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return d.rangeCompleteReplicationTasks(ctx, request)
	default:
		return d.rangeCompleteHistoryTasks(ctx, request)
	}
}

func (d *MutableStateTaskStore) getTransferTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetTransferTasksQuery,
		request.ShardID,
		systemTaskTransfer,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTransferTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeTransferTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteTransferTaskQuery,
		request.ShardID,
		systemTaskTransfer,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteTransferTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteTransferTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	query := d.Session.Query(templateRangeCompleteTransferTaskQuery,
		request.ShardID,
		systemTaskTransfer,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTransferTask", err)
}

func (d *MutableStateTaskStore) getTimerTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could lose tasks
	minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	query := d.Session.Query(templateGetTimerTasksQuery,
		request.ShardID,
		rowTypeTimerTask,
		minTimestamp,
		maxTimestamp,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var timestamp time.Time
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&timestamp, &taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewKey(timestamp, taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		timestamp = time.Time{}
		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTimerTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeTimerTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	ts := p.UnixMilliseconds(request.TaskKey.FireTime)
	query := d.Session.Query(templateCompleteTimerTaskQuery,
		request.ShardID,
		rowTypeTimerTask,
		ts,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteTimerTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteTimerTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	start := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	end := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	query := d.Session.Query(templateRangeCompleteTimerTaskQuery,
		request.ShardID,
		rowTypeTimerTask,
		start,
		end,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTimerTask", err)
}

func (d *MutableStateTaskStore) getReplicationTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading replication tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetReplicationTasksQuery,
		request.ShardID,
		systemTaskReplication,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasks")
}

func (d *MutableStateTaskStore) completeReplicationTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteReplicationTaskQuery,
		request.ShardID,
		systemTaskReplication,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteReplicationTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	query := d.Session.Query(templateRangeCompleteReplicationTaskQuery,
		request.ShardID,
		systemTaskReplication,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) error {
	task := request.TaskInfo
	datablob, err := serialization.ReplicationTaskInfoToBlob(task)
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}

	// Use source cluster name as the workflow id for replication dlq
	query := d.Session.Query(templateCreateReplicationTaskQuery,
		request.ShardID,
		request.SourceClusterName,
		datablob.Data,
		datablob.EncodingType.String(),
		task.GetTaskId(),
	).WithContext(ctx)

	err = query.Exec()
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}

	return nil
}

func (d *MutableStateTaskStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	query := d.Session.Query(templateGetReplicationTasksQuery,
		request.ShardID,
		request.SourceClusterName,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasksFromDLQ")
}

func (d *MutableStateTaskStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {

	query := d.Session.Query(templateCompleteReplicationTaskQuery,
		request.ShardID,
		request.SourceClusterName,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("DeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {

	query := d.Session.Query(templateRangeCompleteReplicationTaskQuery,
		request.ShardID,
		request.SourceClusterName,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeDeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (bool, error) {

	query := d.Session.Query(templateIsQueueEmptyQuery,
		request.ShardID,
		request.SourceClusterName,
		request.InclusiveMinTaskKey.TaskID,
	).WithContext(ctx)

	if err := query.Scan(nil); err != nil {
		if gocql.IsNotFoundError(err) {
			return true, nil
		}
		return true, gocql.ConvertError("IsReplicationDLQEmpty", err)
	}
	return false, nil
}

func (d *MutableStateTaskStore) getVisibilityTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading Visibility tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetVisibilityTasksQuery,
		request.ShardID,
		systemTaskVisibility,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetVisibilityTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeVisibilityTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteVisibilityTaskQuery,
		request.ShardID,
		systemTaskVisibility,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteVisibilityTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteVisibilityTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	query := d.Session.Query(templateRangeCompleteVisibilityTaskQuery,
		request.ShardID,
		systemTaskVisibility,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteVisibilityTask", err)
}

func (d *MutableStateTaskStore) populateGetReplicationTasksResponse(
	query gocql.Query,
	operation string,
) (*p.InternalGetHistoryTasksResponse, error) {
	iter := query.Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError(operation, err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) getHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		return d.getHistoryImmedidateTasks(ctx, request)
	case tasks.CategoryTypeScheduled:
		return d.getHistoryScheduledTasks(ctx, request)
	default:
		panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
	}
}

func (d *MutableStateTaskStore) getHistoryImmedidateTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// execution manager should already validated the request
	// Reading history tasks need to be quorum level consistent, otherwise we could lose task

	query := d.Session.Query(templateGetHistoryImmediateTasksQuery,
		request.ShardID,
		request.TaskCategory.ID(),
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetHistoryImmediateTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) getHistoryScheduledTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// execution manager should already validated the request
	// Reading history tasks need to be quorum level consistent, otherwise we could lose task

	minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	query := d.Session.Query(templateGetHistoryScheduledTasksQuery,
		request.ShardID,
		request.TaskCategory.ID(),
		minTimestamp,
		maxTimestamp,
	).WithContext(ctx)

	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var timestamp time.Time
	var taskID int64
	var data []byte
	var encoding string

	for iter.Scan(&timestamp, &taskID, &data, &encoding) {
		response.Tasks = append(response.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewKey(timestamp, taskID),
			Blob: p.NewDataBlob(data, encoding),
		})

		timestamp = time.Time{}
		taskID = 0
		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetHistoryScheduledTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	ts := defaultVisibilityTimestamp
	if request.TaskCategory.Type() == tasks.CategoryTypeScheduled {
		ts = p.UnixMilliseconds(request.TaskKey.FireTime)
	}
	query := d.Session.Query(templateCompleteHistoryTaskQuery,
		request.ShardID,
		request.TaskCategory.ID(),
		ts,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteHistoryTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	// execution manager should already validated the request
	var query gocql.Query
	if request.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		query = d.Session.Query(templateRangeCompleteHistoryImmediateTasksQuery,
			request.ShardID,
			request.TaskCategory.ID(),
			defaultVisibilityTimestamp,
			request.InclusiveMinTaskKey.TaskID,
			request.ExclusiveMaxTaskKey.TaskID,
		).WithContext(ctx)
	} else {
		minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
		maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
		query = d.Session.Query(templateRangeCompleteHistoryScheduledTasksQuery,
			request.ShardID,
			request.TaskCategory.ID(),
			minTimestamp,
			maxTimestamp,
		).WithContext(ctx)
	}

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteHistoryTasks", err)
}
