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
	"math"
	"sync"

	"github.com/manetu/temporal-yugabyte/utils/gocql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

const (
	templateCreateTaskQuery_v2 = `INSERT INTO tasks_v2 (` +
		`namespace_id, task_queue_name, task_queue_type, type, pass, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?) `

	templateCreateTaskWithTTLQuery_v2 = `INSERT INTO tasks_v2 (` +
		`namespace_id, task_queue_name, task_queue_type, type, pass, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? `

	// YugabyteDB doesn't support multi-column tuple comparisons like (pass, task_id) >= (?, ?)
	// or OR conditions in WHERE clauses. Instead, we use two separate queries that both
	// efficiently use the clustering key index:
	// 1. Tasks in the boundary pass: pass = ? AND task_id >= ?
	// 2. Tasks in subsequent passes: pass > ?
	// Results are merged in application code, prioritizing the boundary pass query.

	// Query for tasks in the boundary pass (uses both clustering columns efficiently)
	templateGetTasksInPassQuery_v2 = `SELECT pass, task_id, task, task_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = ? ` +
		`AND task_id >= ?`

	// Query for tasks in passes after the boundary (uses clustering column efficiently)
	templateGetTasksAfterPassQuery_v2 = `SELECT pass, task_id, task, task_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass > ?`

	// YugabyteDB doesn't support range DELETE with complex conditions
	// So we delete by pass < ? only - this may leave some tasks in the boundary pass
	// but they will be cleaned up on subsequent deletes
	templateCompleteTasksLessThanQuery_v2 = `DELETE FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass < ?`

	// Task queue queries for V2 - stored in tasks_v2 table with pass=0
	templateGetTaskQueueQuery_v2 = `SELECT ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = 0 ` +
		`AND task_id = ?`

	templateInsertTaskQueueQuery_v2 = `INSERT INTO tasks_v2 ` +
		`(namespace_id, task_queue_name, task_queue_type, type, pass, task_id, range_id, task_queue, task_queue_encoding) ` +
		`VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?) IF NOT EXISTS`

	templateUpdateTaskQueueQuery_v2 = `UPDATE tasks_v2 SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = 0 ` +
		`AND task_id = ? ` +
		`IF range_id = ? ELSE ERROR`

	templateDeleteTaskQueueQuery_v2 = `DELETE FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = 0 ` +
		`AND task_id = ? ` +
		`IF range_id = ?`
)

// MatchingTaskStoreV2 implements fair task scheduling using the tasks_v2 table.
// It embeds MatchingTaskStore to reuse user data operations.
type MatchingTaskStoreV2 struct {
	Session gocql.Session
	Logger  log.Logger
	// Embed V1 store for user data operations
	*MatchingTaskStore
}

// NewMatchingTaskStoreV2 creates a new V2 task store for fair scheduling
func NewMatchingTaskStoreV2(
	session gocql.Session,
	logger log.Logger,
) *MatchingTaskStoreV2 {
	return &MatchingTaskStoreV2{
		Session:           session,
		Logger:            logger,
		MatchingTaskStore: NewMatchingTaskStore(session, logger),
	}
}

// rowTypeTaskInSubqueue encodes subqueue index into the type field
// Subqueue 0 must be the same as rowTypeTask (before subqueues were introduced).
// 00000000: task in subqueue 0 (rowTypeTask)
// 00000001: task queue metadata (rowTypeTaskQueue)
// xxxxxx1x: reserved
// 00000100: task in subqueue 1
// nnnnnn00: task in subqueue n, etc.
func rowTypeTaskInSubqueue(subqueue int) int {
	return subqueue<<2 | rowTypeTask
}

// CreateTaskQueue creates a new task queue in tasks_v2
func (d *MatchingTaskStoreV2) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	query := d.Session.Query(templateInsertTaskQueueQuery_v2,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("CreateTaskQueue", err)
	}

	if !applied {
		previousRangeID := previous["range_id"]
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("CreateTaskQueue: TaskQueue:%v, TaskQueueType:%v, PreviousRangeID:%v",
				request.TaskQueue, request.TaskType, previousRangeID),
		}
	}

	return nil
}

// GetTaskQueue retrieves task queue metadata from tasks_v2
func (d *MatchingTaskStoreV2) GetTaskQueue(
	ctx context.Context,
	request *p.InternalGetTaskQueueRequest,
) (*p.InternalGetTaskQueueResponse, error) {
	query := d.Session.Query(templateGetTaskQueueQuery_v2,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
	).WithContext(ctx)

	var rangeID int64
	var tlBytes []byte
	var tlEncoding string
	if err := query.Scan(&rangeID, &tlBytes, &tlEncoding); err != nil {
		return nil, gocql.ConvertError("GetTaskQueue", err)
	}

	return &p.InternalGetTaskQueueResponse{
		RangeID:       rangeID,
		TaskQueueInfo: p.NewDataBlob(tlBytes, tlEncoding),
	}, nil
}

// UpdateTaskQueue updates task queue metadata in tasks_v2
func (d *MatchingTaskStoreV2) UpdateTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	query := d.Session.Query(templateUpdateTaskQueueQuery_v2,
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.PrevRangeID,
	).WithContext(ctx)

	err := query.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			return nil, &p.ConditionFailedError{
				Msg: fmt.Sprintf("Failed to update task queue. name: %v, type: %v, rangeID: %v",
					request.TaskQueue, request.TaskType, request.RangeID),
			}
		}

		return nil, gocql.ConvertError("UpdateTaskQueue", err)
	}

	return &p.UpdateTaskQueueResponse{}, nil
}

// DeleteTaskQueue deletes a task queue from tasks_v2
func (d *MatchingTaskStoreV2) DeleteTaskQueue(
	ctx context.Context,
	request *p.DeleteTaskQueueRequest,
) error {
	query := d.Session.Query(
		templateDeleteTaskQueueQuery_v2,
		request.TaskQueue.NamespaceID,
		request.TaskQueue.TaskQueueName,
		request.TaskQueue.TaskQueueType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.RangeID,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("DeleteTaskQueue", err)
	}
	if !applied {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("DeleteTaskQueue operation failed: expected_range_id=%v but found %+v", request.RangeID, previous),
		}
	}
	return nil
}

// CreateTasks adds tasks with fair scheduling support
func (d *MatchingTaskStoreV2) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	txn := d.Session.NewTxn().WithContext(ctx)
	namespaceID := request.NamespaceID
	taskQueue := request.TaskQueue
	taskQueueType := request.TaskType

	for _, task := range request.Tasks {
		if task.TaskPass == 0 {
			return nil, serviceerror.NewInternal("invalid fair queue task missing pass number")
		}

		ttl := GetTaskTTL(task.ExpiryTime)

		if ttl <= 0 || ttl > maxCassandraTTL {
			txn.Query(templateCreateTaskQuery_v2,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTaskInSubqueue(task.Subqueue),
				task.TaskPass,
				task.TaskId,
				task.Task.Data,
				task.Task.EncodingType.String())
		} else {
			txn.Query(templateCreateTaskWithTTLQuery_v2,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTaskInSubqueue(task.Subqueue),
				task.TaskPass,
				task.TaskId,
				task.Task.Data,
				task.Task.EncodingType.String(),
				ttl)
		}
	}

	// The following query is used to ensure that range_id didn't change
	txn.Query(templateUpdateTaskQueueQuery_v2,
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
		namespaceID,
		taskQueue,
		taskQueueType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.RangeID,
	)

	err := txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			return nil, &p.ConditionFailedError{
				Msg: fmt.Sprintf("Failed to create task. TaskQueue: %v, taskQueueType: %v, rangeID: %v",
					taskQueue, taskQueueType, request.RangeID),
			}
		}

		return nil, err
	}

	return &p.CreateTasksResponse{}, nil
}

// taskResult holds a single task's data for merging query results
type taskResult struct {
	pass     int64
	taskID   int64
	task     []byte
	encoding string
}

// GetTasks retrieves tasks using fair scheduling (pass-based ordering).
// YugabyteDB doesn't support tuple comparisons like (pass, task_id) >= (?, ?),
// so we run two parallel queries that both efficiently use the clustering key:
// 1. Boundary pass: pass = minPass AND task_id >= minTaskID
// 2. Subsequent passes: pass > minPass
// Results are merged with boundary pass tasks taking priority (lower order values).
func (d *MatchingTaskStoreV2) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (*p.InternalGetTasksResponse, error) {
	if request.InclusiveMinPass < 1 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: InclusiveMinPass must be >= 1")
	}
	if request.ExclusiveMaxTaskID != math.MaxInt64 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: ExclusiveMaxTaskID is not supported")
	}

	// Run both queries in parallel - both use clustering key efficiently
	var wg sync.WaitGroup
	var boundaryResults []taskResult
	var subsequentResults []taskResult
	var boundaryErr, subsequentErr error

	pageSize := request.PageSize
	if pageSize <= 0 {
		pageSize = 100 // default page size
	}

	// Query 1: Tasks in the boundary pass (pass = minPass AND task_id >= minTaskID)
	wg.Add(1)
	go func() {
		defer wg.Done()
		query := d.Session.Query(templateGetTasksInPassQuery_v2,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskInSubqueue(request.Subqueue),
			request.InclusiveMinPass,
			request.InclusiveMinTaskID,
		).WithContext(ctx)
		iter := query.PageSize(pageSize).Iter()
		boundaryResults, boundaryErr = d.collectTaskResults(iter, pageSize)
	}()

	// Query 2: Tasks in subsequent passes (pass > minPass)
	wg.Add(1)
	go func() {
		defer wg.Done()
		query := d.Session.Query(templateGetTasksAfterPassQuery_v2,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskInSubqueue(request.Subqueue),
			request.InclusiveMinPass,
		).WithContext(ctx)
		iter := query.PageSize(pageSize).Iter()
		subsequentResults, subsequentErr = d.collectTaskResults(iter, pageSize)
	}()

	wg.Wait()

	// Check for errors
	if boundaryErr != nil {
		return nil, boundaryErr
	}
	if subsequentErr != nil {
		return nil, subsequentErr
	}

	// Merge results: boundary pass tasks come first (they have lower pass values)
	// Then subsequent pass tasks (already ordered by pass, task_id from the query)
	response := &p.InternalGetTasksResponse{}
	collected := 0

	// First, add boundary pass results
	for _, tr := range boundaryResults {
		if collected >= pageSize {
			break
		}
		response.Tasks = append(response.Tasks, p.NewDataBlob(tr.task, tr.encoding))
		collected++
	}

	// Then, add subsequent pass results if we need more
	for _, tr := range subsequentResults {
		if collected >= pageSize {
			break
		}
		response.Tasks = append(response.Tasks, p.NewDataBlob(tr.task, tr.encoding))
		collected++
	}

	// Note: We don't use NextPageToken with the two-query approach.
	// The caller tracks progress via InclusiveMinPass/InclusiveMinTaskID.

	return response, nil
}

// collectTaskResults reads task results from an iterator up to maxResults
func (d *MatchingTaskStoreV2) collectTaskResults(iter gocql.Iter, maxResults int) ([]taskResult, error) {
	var results []taskResult
	task := make(map[string]interface{})

	for iter.MapScan(task) {
		if len(results) >= maxResults {
			break
		}

		// Extract pass
		rawPass, ok := task["pass"]
		if !ok {
			task = make(map[string]interface{})
			continue
		}
		pass, ok := rawPass.(int64)
		if !ok {
			task = make(map[string]interface{})
			continue
		}

		// Extract task_id
		rawTaskID, ok := task["task_id"]
		if !ok {
			task = make(map[string]interface{})
			continue
		}
		taskID, ok := rawTaskID.(int64)
		if !ok {
			task = make(map[string]interface{})
			continue
		}

		// Extract task blob
		rawTask, ok := task["task"]
		if !ok {
			_ = iter.Close()
			return nil, newFieldNotFoundError("task", task)
		}
		taskVal, ok := rawTask.([]byte)
		if !ok {
			_ = iter.Close()
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task", byteSliceType, rawTask, task)
		}

		// Extract encoding
		rawEncoding, ok := task["task_encoding"]
		if !ok {
			_ = iter.Close()
			return nil, newFieldNotFoundError("task_encoding", task)
		}
		encodingVal, ok := rawEncoding.(string)
		if !ok {
			_ = iter.Close()
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task_encoding", byteSliceType, rawEncoding, task)
		}

		results = append(results, taskResult{
			pass:     pass,
			taskID:   taskID,
			task:     taskVal,
			encoding: encodingVal,
		})

		task = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTasks query failed: %v", err))
	}

	return results, nil
}

// CompleteTasksLessThan deletes tasks using fair scheduling (pass-based ordering)
// Note: YugabyteDB doesn't support complex range deletes, so we only delete by pass < ExclusiveMaxPass.
// Tasks in the boundary pass will be cleaned up on subsequent calls.
func (d *MatchingTaskStoreV2) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	if request.ExclusiveMaxPass < 1 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on fair queue")
	}

	query := d.Session.Query(
		templateCompleteTasksLessThanQuery_v2,
		request.NamespaceID,
		request.TaskQueueName,
		request.TaskType,
		rowTypeTaskInSubqueue(request.Subqueue),
		request.ExclusiveMaxPass,
	).WithContext(ctx)

	err := query.Exec()
	if err != nil {
		return 0, gocql.ConvertError("CompleteTasksLessThan", err)
	}
	return p.UnknownNumRowsAffected, nil
}

// GetName returns the persistence name
func (d *MatchingTaskStoreV2) GetName() string {
	return yugabytePersistenceName
}

// Close closes the session
func (d *MatchingTaskStoreV2) Close() {
	if d.Session != nil {
		d.Session.Close()
	}
}
