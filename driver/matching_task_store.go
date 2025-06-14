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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?) `

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?) USING TTL ? `

	templateGetTasksQuery = `SELECT task_id, task, task_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateCompleteTasksLessThanQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id < ? `

	templateGetTaskQueueQuery = `SELECT ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskQueueQuery = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateUpdateTaskQueueQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ? ELSE ERROR `

	templateUpdateTaskQueueQueryWithTTLPart1 = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id ` +
		`) VALUES (?, ?, ?, ?, ?) USING TTL ? `

	templateUpdateTaskQueueQueryWithTTLPart2 = `UPDATE tasks USING TTL ? SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ? ELSE ERROR `

	templateDeleteTaskQueueQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id = ? ` +
		`IF range_id = ?`

	templateGetTaskQueueUserDataQuery = `SELECT data, data_encoding, version
	    FROM task_queue_user_data
		WHERE namespace_id = ? AND build_id = ''
		AND task_queue_name = ?`

	templateUpdateTaskQueueUserDataQuery = `UPDATE task_queue_user_data SET
		data = ?,
		data_encoding = ?,
		version = ?
		WHERE namespace_id = ?
		AND build_id = ''
		AND task_queue_name = ?
		IF version = ? ELSE ERROR`

	templateInsertTaskQueueUserDataQuery = `INSERT INTO task_queue_user_data
		(namespace_id, build_id, task_queue_name, data, data_encoding, version) VALUES
		(?           , ''      , ?              , ?   , ?            , 1      ) IF NOT EXISTS ELSE ERROR`

	templateInsertBuildIdTaskQueueMappingQuery = `INSERT INTO task_queue_user_data
	(namespace_id, build_id, task_queue_name) VALUES
	(?           , ?       , ?)`
	templateDeleteBuildIdTaskQueueMappingQuery = `DELETE FROM task_queue_user_data
	WHERE namespace_id = ? AND build_id = ? AND task_queue_name = ?`
	templateListTaskQueueUserDataQuery       = `SELECT task_queue_name, data, data_encoding, version FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ''`
	templateListTaskQueueNamesByBuildIdQuery = `SELECT task_queue_name FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ?`
	templateCountTaskQueueByBuildIdQuery     = `SELECT COUNT(*) FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ?`

	// Not much of a need to make this configurable, we're just reading some strings
	listTaskQueueNamesByBuildIdPageSize = 100
)

type (
	MatchingTaskStore struct {
		Session gocql.Session
		Logger  log.Logger
	}
)

func NewMatchingTaskStore(
	session gocql.Session,
	logger log.Logger,
) *MatchingTaskStore {
	return &MatchingTaskStore{
		Session: session,
		Logger:  logger,
	}
}

func (d *MatchingTaskStore) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	query := d.Session.Query(templateInsertTaskQueueQuery,
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

func (d *MatchingTaskStore) GetTaskQueue(
	ctx context.Context,
	request *p.InternalGetTaskQueueRequest,
) (*p.InternalGetTaskQueueResponse, error) {
	query := d.Session.Query(templateGetTaskQueueQuery,
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

// UpdateTaskQueue update task queue
func (d *MatchingTaskStore) UpdateTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	var err error
	if request.TaskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY { // if task_queue is sticky, then update with TTL
		if request.ExpiryTime == nil {
			return nil, serviceerror.NewInternal("ExpiryTime cannot be nil for sticky task queue")
		}
		expiryTTL := convert.Int64Ceil(time.Until(timestamp.TimeValue(request.ExpiryTime)).Seconds())
		if expiryTTL >= maxCassandraTTL {
			expiryTTL = maxCassandraTTL
		}
		txn := d.Session.NewTxn().WithContext(ctx)
		txn.Query(templateUpdateTaskQueueQueryWithTTLPart1,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			expiryTTL,
		)
		txn.Query(templateUpdateTaskQueueQueryWithTTLPart2,
			expiryTTL,
			request.RangeID,
			request.TaskQueueInfo.Data,
			request.TaskQueueInfo.EncodingType.String(),
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			request.PrevRangeID,
		)
		err = txn.Exec()
	} else {
		query := d.Session.Query(templateUpdateTaskQueueQuery,
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
		err = query.Exec()
	}

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

func (d *MatchingTaskStore) ListTaskQueue(
	_ context.Context,
	_ *p.ListTaskQueueRequest,
) (*p.InternalListTaskQueueResponse, error) {
	return nil, serviceerror.NewUnavailable("unsupported operation")
}

func (d *MatchingTaskStore) DeleteTaskQueue(
	ctx context.Context,
	request *p.DeleteTaskQueueRequest,
) error {
	query := d.Session.Query(
		templateDeleteTaskQueueQuery,
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

// CreateTasks add tasks
func (d *MatchingTaskStore) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	txn := d.Session.NewTxn().WithContext(ctx)
	namespaceID := request.NamespaceID
	taskQueue := request.TaskQueue
	taskQueueType := request.TaskType

	for _, task := range request.Tasks {
		ttl := GetTaskTTL(task.ExpiryTime)

		if ttl <= 0 || ttl > maxCassandraTTL {
			txn.Query(templateCreateTaskQuery,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTask,
				task.TaskId,
				task.Task.Data,
				task.Task.EncodingType.String())
		} else {
			txn.Query(templateCreateTaskWithTTLQuery,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTask,
				task.TaskId,
				task.Task.Data,
				task.Task.EncodingType.String(),
				ttl)
		}
	}

	// The following query is used to ensure that range_id didn't change
	txn.Query(templateUpdateTaskQueueQuery,
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

func GetTaskTTL(expireTime *timestamppb.Timestamp) int64 {
	var ttl int64 = 0
	if expireTime != nil && !expireTime.AsTime().IsZero() {
		expiryTtl := convert.Int64Ceil(time.Until(expireTime.AsTime()).Seconds())

		// 0 means no ttl, we dont want that.
		// Todo: Come back and correctly ignore expired in-memory tasks before persisting
		if expiryTtl < 1 {
			expiryTtl = 1
		}

		ttl = expiryTtl
	}
	return ttl
}

// GetTasks get a task
func (d *MatchingTaskStore) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (*p.InternalGetTasksResponse, error) {
	// Reading taskqueue tasks need to be quorum level consistent, otherwise we could lose tasks
	query := d.Session.Query(templateGetTasksQuery,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTask,
		request.InclusiveMinTaskID,
		request.ExclusiveMaxTaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		_, ok := task["task_id"]
		if !ok { // no tasks, but static column record returned
			continue
		}

		rawTask, ok := task["task"]
		if !ok {
			return nil, newFieldNotFoundError("task", task)
		}
		taskVal, ok := rawTask.([]byte)
		if !ok {
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task", byteSliceType, rawTask, task)

		}

		rawEncoding, ok := task["task_encoding"]
		if !ok {
			return nil, newFieldNotFoundError("task_encoding", task)
		}
		encodingVal, ok := rawEncoding.(string)
		if !ok {
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task_encoding", byteSliceType, rawEncoding, task)
		}
		response.Tasks = append(response.Tasks, p.NewDataBlob(taskVal, encodingVal))

		task = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTasks operation failed. Error: %v", err))
	}
	return response, nil
}

// CompleteTasksLessThan deletes all tasks less than the given task id. This API ignores the
// Limit request parameter i.e. either all tasks leq the task_id will be deleted or an error will
// be returned to the caller
func (d *MatchingTaskStore) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	query := d.Session.Query(
		templateCompleteTasksLessThanQuery,
		request.NamespaceID,
		request.TaskQueueName,
		request.TaskType,
		rowTypeTask,
		request.ExclusiveMaxTaskID,
	).WithContext(ctx)
	err := query.Exec()
	if err != nil {
		return 0, gocql.ConvertError("CompleteTasksLessThan", err)
	}
	return p.UnknownNumRowsAffected, nil
}

func (d *MatchingTaskStore) GetTaskQueueUserData(
	ctx context.Context,
	request *p.GetTaskQueueUserDataRequest,
) (*p.InternalGetTaskQueueUserDataResponse, error) {
	query := d.Session.Query(templateGetTaskQueueUserDataQuery,
		request.NamespaceID,
		request.TaskQueue,
	).WithContext(ctx)
	var version int64
	var userDataBytes []byte
	var encoding string
	if err := query.Scan(&userDataBytes, &encoding, &version); err != nil {
		return nil, gocql.ConvertError("GetTaskQueueData", err)
	}

	return &p.InternalGetTaskQueueUserDataResponse{
		Version:  version,
		UserData: p.NewDataBlob(userDataBytes, encoding),
	}, nil
}

func (d *MatchingTaskStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) error {
	txn := d.Session.NewTxn().WithContext(ctx)

	for taskQueue, update := range request.Updates {
		if update.Version == 0 {
			txn.Query(templateInsertTaskQueueUserDataQuery,
				request.NamespaceID,
				taskQueue,
				update.UserData.Data,
				update.UserData.EncodingType.String(),
			)
		} else {
			txn.Query(templateUpdateTaskQueueUserDataQuery,
				update.UserData.Data,
				update.UserData.EncodingType.String(),
				update.Version+1,
				request.NamespaceID,
				taskQueue,
				update.Version,
			)
		}
		for _, buildId := range update.BuildIdsAdded {
			txn.Query(templateInsertBuildIdTaskQueueMappingQuery, request.NamespaceID, buildId, taskQueue)
		}
		for _, buildId := range update.BuildIdsRemoved {
			txn.Query(templateDeleteBuildIdTaskQueueMappingQuery, request.NamespaceID, buildId, taskQueue)
		}
	}

	err := txn.Exec()
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = err == nil
		}
	}

	if err != nil {
		if gocql.ConflictError(err) {
			for _, update := range request.Updates {
				if update.Conflicting != nil {
					*update.Conflicting = true
				}
			}

			return &p.ConditionFailedError{Msg: "Failed to update task queues: unknown conflict"}
		}

		return err
	}

	return nil
}

func (d *MatchingTaskStore) ListTaskQueueUserDataEntries(ctx context.Context, request *p.ListTaskQueueUserDataEntriesRequest) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	query := d.Session.Query(templateListTaskQueueUserDataQuery, request.NamespaceID).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalListTaskQueueUserDataEntriesResponse{}
	row := make(map[string]interface{})
	for iter.MapScan(row) {
		taskQueue, err := getTypedFieldFromRow[string]("task_queue_name", row)
		if err != nil {
			return nil, err
		}
		data, err := getTypedFieldFromRow[[]byte]("data", row)
		if err != nil {
			return nil, err
		}
		dataEncoding, err := getTypedFieldFromRow[string]("data_encoding", row)
		if err != nil {
			return nil, err
		}
		version, err := getTypedFieldFromRow[int64]("version", row)
		if err != nil {
			return nil, err
		}

		response.Entries = append(response.Entries, p.InternalTaskQueueUserDataEntry{TaskQueue: taskQueue, Data: p.NewDataBlob(data, dataEncoding), Version: version})

		row = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListTaskQueueUserDataEntries operation failed. Error: %v", err))
	}
	return response, nil
}

func (d *MatchingTaskStore) GetTaskQueuesByBuildId(ctx context.Context, request *p.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	query := d.Session.Query(templateListTaskQueueNamesByBuildIdQuery, request.NamespaceID, request.BuildID).WithContext(ctx)
	iter := query.PageSize(listTaskQueueNamesByBuildIdPageSize).Iter()

	var taskQueues []string
	row := make(map[string]interface{})

	for {
		for iter.MapScan(row) {
			taskQueueRaw, ok := row["task_queue_name"]
			if !ok {
				return nil, newFieldNotFoundError("task_queue_name", row)
			}
			taskQueue, ok := taskQueueRaw.(string)
			if !ok {
				var stringType string
				return nil, newPersistedTypeMismatchError("task_queue_name", stringType, taskQueueRaw, row)
			}

			taskQueues = append(taskQueues, taskQueue)

			row = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
		}
		if len(iter.PageState()) == 0 {
			break
		}
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTaskQueuesByBuildId operation failed. Error: %v", err))
	}
	return taskQueues, nil
}

func (d *MatchingTaskStore) CountTaskQueuesByBuildId(ctx context.Context, request *p.CountTaskQueuesByBuildIdRequest) (int, error) {
	var count int
	query := d.Session.Query(templateCountTaskQueueByBuildIdQuery, request.NamespaceID, request.BuildID).WithContext(ctx)
	err := query.Scan(&count)
	return count, err
}

func (d *MatchingTaskStore) GetName() string {
	return yugabytePersistenceName
}

func (d *MatchingTaskStore) Close() {
	if d.Session != nil {
		d.Session.Close()
	}
}
