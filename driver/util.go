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
	"github.com/manetu/temporal-yugabyte/utils/gocql"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	p "go.temporal.io/server/common/persistence"

	"go.temporal.io/server/service/history/tasks"
)

func applyWorkflowMutationTxn(
	txn *gocql.Txn,
	shardID int32,
	workflowMutation *p.InternalWorkflowMutation,
) error {

	// TODO update all call sites to update LastUpdatetime
	// cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowMutation.NamespaceID
	workflowID := workflowMutation.WorkflowID
	runID := workflowMutation.RunID

	if err := updateExecution(
		txn,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowMutation.ExecutionInfoBlob,
		workflowMutation.ExecutionState,
		workflowMutation.ExecutionStateBlob,
		workflowMutation.NextEventID,
		workflowMutation.Condition,
		workflowMutation.DBRecordVersion,
		workflowMutation.Checksum,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		txn,
		workflowMutation.UpsertActivityInfos,
		workflowMutation.DeleteActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		txn,
		workflowMutation.UpsertTimerInfos,
		workflowMutation.DeleteTimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		txn,
		workflowMutation.UpsertChildExecutionInfos,
		workflowMutation.DeleteChildExecutionInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		txn,
		workflowMutation.UpsertRequestCancelInfos,
		workflowMutation.DeleteRequestCancelInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		txn,
		workflowMutation.UpsertSignalInfos,
		workflowMutation.DeleteSignalInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		txn,
		workflowMutation.UpsertSignalRequestedIDs,
		workflowMutation.DeleteSignalRequestedIDs,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	updateBufferedEvents(
		txn,
		workflowMutation.NewBufferedEvents,
		workflowMutation.ClearBufferedEvents,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	updateChasmNodes(
		txn,
		workflowMutation.UpsertChasmNodes,
		workflowMutation.DeleteChasmNodes,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		txn,
		shardID,
		workflowMutation.Tasks,
	)
}

func applyWorkflowSnapshotTxnAsReset(
	txn *gocql.Txn,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {

	// TODO: update call site
	// cqlNowTimestampMillis := p.UnixMilliseconds(time.Now().UTC())

	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := updateExecution(
		txn,
		shardID,
		namespaceID,
		workflowID,
		runID,
		workflowSnapshot.ExecutionInfoBlob,
		workflowSnapshot.ExecutionState,
		workflowSnapshot.ExecutionStateBlob,
		workflowSnapshot.NextEventID,
		workflowSnapshot.Condition,
		workflowSnapshot.DBRecordVersion,
		workflowSnapshot.Checksum,
	); err != nil {
		return err
	}

	if err := resetActivityInfos(
		txn,
		workflowSnapshot.ActivityInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetTimerInfos(
		txn,
		workflowSnapshot.TimerInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetChildExecutionInfos(
		txn,
		workflowSnapshot.ChildExecutionInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetRequestCancelInfos(
		txn,
		workflowSnapshot.RequestCancelInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := resetSignalInfos(
		txn,
		workflowSnapshot.SignalInfos,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	resetSignalRequested(
		txn,
		workflowSnapshot.SignalRequestedIDs,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	deleteBufferedEvents(
		txn,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	resetChasmNodes(
		txn,
		workflowSnapshot.ChasmNodes,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		txn,
		shardID,
		workflowSnapshot.Tasks,
	)
}

func applyWorkflowSnapshotTxnAsNew(
	txn *gocql.Txn,
	shardID int32,
	workflowSnapshot *p.InternalWorkflowSnapshot,
) error {
	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID

	if err := createExecution(
		txn,
		shardID,
		workflowSnapshot,
	); err != nil {
		return err
	}

	if err := updateActivityInfos(
		txn,
		workflowSnapshot.ActivityInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateTimerInfos(
		txn,
		workflowSnapshot.TimerInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateChildExecutionInfos(
		txn,
		workflowSnapshot.ChildExecutionInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateRequestCancelInfos(
		txn,
		workflowSnapshot.RequestCancelInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	if err := updateSignalInfos(
		txn,
		workflowSnapshot.SignalInfos,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	); err != nil {
		return err
	}

	updateSignalsRequested(
		txn,
		workflowSnapshot.SignalRequestedIDs,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	updateChasmNodes(
		txn,
		workflowSnapshot.ChasmNodes,
		nil,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)

	// transfer / replication / timer tasks
	return applyTasks(
		txn,
		shardID,
		workflowSnapshot.Tasks,
	)
}

func createExecution(
	txn *gocql.Txn,
	shardID int32,
	snapshot *p.InternalWorkflowSnapshot,
) error {
	// validate workflow state & close status
	if err := p.ValidateCreateWorkflowStateStatus(
		snapshot.ExecutionState.State,
		snapshot.ExecutionState.Status); err != nil {
		return err
	}

	// TODO also need to set the start / current / last write version
	txn.Query(templateCreateWorkflowExecutionQuery,
		shardID,
		snapshot.NamespaceID,
		snapshot.WorkflowID,
		snapshot.RunID,
		snapshot.ExecutionInfoBlob.Data,
		snapshot.ExecutionInfoBlob.EncodingType.String(),
		snapshot.ExecutionStateBlob.Data,
		snapshot.ExecutionStateBlob.EncodingType.String(),
		snapshot.NextEventID,
		snapshot.DBRecordVersion,
		snapshot.Checksum.Data,
		snapshot.Checksum.EncodingType.String(),
	)

	return nil
}

func updateExecution(
	txn *gocql.Txn,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	executionInfoBlob *commonpb.DataBlob,
	executionState *persistencespb.WorkflowExecutionState,
	executionStateBlob *commonpb.DataBlob,
	nextEventID int64,
	condition int64,
	dbRecordVersion int64,
	checksumBlob *commonpb.DataBlob,
) error {

	// validate workflow state & close status
	if err := p.ValidateUpdateWorkflowStateStatus(
		executionState.State,
		executionState.Status); err != nil {
		return err
	}

	if dbRecordVersion == 0 {
		txn.Query(templateUpdateWorkflowExecutionQueryDeprecated,
			executionInfoBlob.Data,
			executionInfoBlob.EncodingType.String(),
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			nextEventID,
			dbRecordVersion,
			checksumBlob.Data,
			checksumBlob.EncodingType.String(),
			shardID,
			namespaceID,
			workflowID,
			runID,
			condition,
		)
	} else {
		txn.Query(templateUpdateWorkflowExecutionQuery,
			executionInfoBlob.Data,
			executionInfoBlob.EncodingType.String(),
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			nextEventID,
			dbRecordVersion,
			checksumBlob.Data,
			checksumBlob.EncodingType.String(),
			shardID,
			namespaceID,
			workflowID,
			runID,
			dbRecordVersion-1,
		)
	}

	return nil
}

func applyTasks(
	txn *gocql.Txn,
	shardID int32,
	insertTasks map[tasks.Category][]p.InternalHistoryTask,
) error {

	var err error
	for category, tasksByCategory := range insertTasks {
		switch category.ID() {
		case tasks.CategoryIDTransfer:
			err = createTransferTasks(txn, tasksByCategory, shardID)
		case tasks.CategoryIDTimer:
			err = createTimerTasks(txn, tasksByCategory, shardID)
		case tasks.CategoryIDVisibility:
			err = createVisibilityTasks(txn, tasksByCategory, shardID)
		case tasks.CategoryIDReplication:
			err = createReplicationTasks(txn, tasksByCategory, shardID)
		default:
			err = createHistoryTasks(txn, category, tasksByCategory, shardID)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func createTransferTasks(
	txn *gocql.Txn,
	transferTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range transferTasks {
		txn.Query(templateCreateTransferTaskQuery,
			shardID,
			systemTaskTransfer,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			task.Key.TaskID,
		)
	}
	return nil
}

func createTimerTasks(
	txn *gocql.Txn,
	timerTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range timerTasks {
		txn.Query(templateCreateTimerTaskQuery,
			shardID,
			rowTypeTimerTask,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			p.UnixMilliseconds(task.Key.FireTime),
			task.Key.TaskID,
		)
	}
	return nil
}

func createReplicationTasks(
	txn *gocql.Txn,
	replicationTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range replicationTasks {
		txn.Query(templateCreateReplicationTaskQuery,
			shardID,
			systemTaskReplication,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			task.Key.TaskID,
		)
	}
	return nil
}

func createVisibilityTasks(
	txn *gocql.Txn,
	visibilityTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	for _, task := range visibilityTasks {
		txn.Query(templateCreateVisibilityTaskQuery,
			shardID,
			systemTaskVisibility,
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			task.Key.TaskID,
		)
	}
	return nil
}

func createHistoryTasks(
	txn *gocql.Txn,
	category tasks.Category,
	historyTasks []p.InternalHistoryTask,
	shardID int32,
) error {
	isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
	for _, task := range historyTasks {
		visibilityTimestamp := defaultVisibilityTimestamp
		if isScheduledTask {
			visibilityTimestamp = p.UnixMilliseconds(task.Key.FireTime)
		}
		txn.Query(templateCreateHistoryTaskQuery,
			shardID,
			category.ID(),
			task.Blob.Data,
			task.Blob.EncodingType.String(),
			visibilityTimestamp,
			task.Key.TaskID,
		)
	}
	return nil
}

func updateActivityInfos(
	txn *gocql.Txn,
	activityInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for scheduledEventID, blob := range activityInfos {
		txn.Query(templateUpdateActivityInfoQuery,
			scheduledEventID,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			namespaceID,
			workflowID,
			runID)
	}

	for deleteID := range deleteIDs {
		txn.Query(templateDeleteActivityInfoQuery,
			deleteID,
			shardID,
			namespaceID,
			workflowID,
			runID)
	}
	return nil
}

func deleteBufferedEvents(
	txn *gocql.Txn,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {
	txn.Query(templateDeleteBufferedEventsQuery,
		shardID,
		namespaceID,
		workflowID,
		runID,
	)
}

func resetActivityInfos(
	txn *gocql.Txn,
	activityInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	infoMap, encoding, err := resetActivityInfoMap(activityInfos)
	if err != nil {
		return err
	}

	txn.Query(templateResetActivityInfoQuery,
		infoMap,
		encoding.String(),
		shardID,
		namespaceID,
		workflowID,
		runID)
	return nil
}

func updateTimerInfos(
	txn *gocql.Txn,
	timerInfos map[string]*commonpb.DataBlob,
	deleteInfos map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	for timerID, blob := range timerInfos {
		txn.Query(templateUpdateTimerInfoQuery,
			timerID,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			namespaceID,
			workflowID,
			runID)
	}

	for deleteInfoID := range deleteInfos {
		txn.Query(templateDeleteTimerInfoQuery,
			deleteInfoID,
			shardID,
			namespaceID,
			workflowID,
			runID)
	}

	return nil
}

func resetTimerInfos(
	txn *gocql.Txn,
	timerInfos map[string]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	timerMap, timerMapEncoding, err := resetTimerInfoMap(timerInfos)
	if err != nil {
		return err
	}

	txn.Query(templateResetTimerInfoQuery,
		timerMap,
		timerMapEncoding.String(),
		shardID,
		namespaceID,
		workflowID,
		runID)

	return nil
}

func updateChildExecutionInfos(
	txn *gocql.Txn,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range childExecutionInfos {
		txn.Query(templateUpdateChildExecutionInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			namespaceID,
			workflowID,
			runID)
	}

	for deleteID := range deleteIDs {
		txn.Query(templateDeleteChildExecutionInfoQuery,
			deleteID,
			shardID,
			namespaceID,
			workflowID,
			runID)
	}
	return nil
}

func resetChildExecutionInfos(
	txn *gocql.Txn,
	childExecutionInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	infoMap, encoding, err := resetChildExecutionInfoMap(childExecutionInfos)
	if err != nil {
		return err
	}
	txn.Query(templateResetChildExecutionInfoQuery,
		infoMap,
		encoding.String(),
		shardID,
		namespaceID,
		workflowID,
		runID)
	return nil
}

func updateRequestCancelInfos(
	txn *gocql.Txn,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range requestCancelInfos {
		txn.Query(templateUpdateRequestCancelInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			namespaceID,
			workflowID,
			runID)
	}

	for deleteID := range deleteIDs {
		txn.Query(templateDeleteRequestCancelInfoQuery,
			deleteID,
			shardID,
			namespaceID,
			workflowID,
			runID)
	}
	return nil
}

func resetRequestCancelInfos(
	txn *gocql.Txn,
	requestCancelInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	rciMap, rciMapEncoding, err := resetRequestCancelInfoMap(requestCancelInfos)

	if err != nil {
		return err
	}

	txn.Query(templateResetRequestCancelInfoQuery,
		rciMap,
		rciMapEncoding.String(),
		shardID,
		namespaceID,
		workflowID,
		runID)

	return nil
}

func updateSignalInfos(
	txn *gocql.Txn,
	signalInfos map[int64]*commonpb.DataBlob,
	deleteIDs map[int64]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {

	for initiatedId, blob := range signalInfos {
		txn.Query(templateUpdateSignalInfoQuery,
			initiatedId,
			blob.Data,
			blob.EncodingType.String(),
			shardID,
			namespaceID,
			workflowID,
			runID)
	}

	for deleteID := range deleteIDs {
		txn.Query(templateDeleteSignalInfoQuery,
			deleteID,
			shardID,
			namespaceID,
			workflowID,
			runID)
	}
	return nil
}

func resetSignalInfos(
	txn *gocql.Txn,
	signalInfos map[int64]*commonpb.DataBlob,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	sMap, sMapEncoding, err := resetSignalInfoMap(signalInfos)

	if err != nil {
		return err
	}

	txn.Query(templateResetSignalInfoQuery,
		sMap,
		sMapEncoding.String(),
		shardID,
		namespaceID,
		workflowID,
		runID)

	return nil
}

func updateSignalsRequested(
	txn *gocql.Txn,
	signalReqIDs map[string]struct{},
	deleteSignalReqIDs map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if len(signalReqIDs) > 0 {
		txn.Query(templateUpdateSignalRequestedQuery,
			convert.StringSetToSlice(signalReqIDs),
			shardID,
			namespaceID,
			workflowID,
			runID)
	}

	if len(deleteSignalReqIDs) > 0 {
		txn.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			convert.StringSetToSlice(deleteSignalReqIDs),
			shardID,
			namespaceID,
			workflowID,
			runID)
	}
}

func resetSignalRequested(
	txn *gocql.Txn,
	signalRequested map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	txn.Query(templateResetSignalRequestedQuery,
		convert.StringSetToSlice(signalRequested),
		shardID,
		namespaceID,
		workflowID,
		runID)
}

func updateBufferedEvents(
	txn *gocql.Txn,
	newBufferedEvents *commonpb.DataBlob,
	clearBufferedEvents bool,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {

	if clearBufferedEvents {
		txn.Query(templateDeleteBufferedEventsQuery,
			shardID,
			namespaceID,
			workflowID,
			runID)
	} else if newBufferedEvents != nil {
		values := make(map[string]interface{})
		values["encoding_type"] = newBufferedEvents.EncodingType.String()
		values["version"] = int64(0)
		values["data"] = newBufferedEvents.Data
		newEventValues := []map[string]interface{}{values}
		txn.Query(templateAppendBufferedEventsQuery,
			newEventValues,
			shardID,
			namespaceID,
			workflowID,
			runID)
	}
}

func resetActivityInfoMap(
	activityInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	aMap := make(map[int64][]byte)
	for scheduledEventID, blob := range activityInfos {
		aMap[scheduledEventID] = blob.Data
		encoding = blob.EncodingType
	}

	return aMap, encoding, nil
}

func resetTimerInfoMap(
	timerInfos map[string]*commonpb.DataBlob,
) (map[string][]byte, enumspb.EncodingType, error) {

	tMap := make(map[string][]byte)
	var encoding enumspb.EncodingType
	for timerID, blob := range timerInfos {
		encoding = blob.EncodingType
		tMap[timerID] = blob.Data
	}

	return tMap, encoding, nil
}

func resetChildExecutionInfoMap(
	childExecutionInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	cMap := make(map[int64][]byte)
	encoding := enumspb.ENCODING_TYPE_UNSPECIFIED
	for initiatedID, blob := range childExecutionInfos {
		cMap[initiatedID] = blob.Data
		encoding = blob.EncodingType
	}

	return cMap, encoding, nil
}

func resetRequestCancelInfoMap(
	requestCancelInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	rcMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for initiatedID, blob := range requestCancelInfos {
		encoding = blob.EncodingType
		rcMap[initiatedID] = blob.Data
	}

	return rcMap, encoding, nil
}

func resetSignalInfoMap(
	signalInfos map[int64]*commonpb.DataBlob,
) (map[int64][]byte, enumspb.EncodingType, error) {

	sMap := make(map[int64][]byte)
	var encoding enumspb.EncodingType
	for initiatedID, blob := range signalInfos {
		encoding = blob.EncodingType
		sMap[initiatedID] = blob.Data
	}

	return sMap, encoding, nil
}

func createHistoryEventBatchBlob(
	result map[string]interface{},
) *commonpb.DataBlob {
	eventBatch := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_UNSPECIFIED}
	for k, v := range result {
		switch k {
		case "encoding_type":
			encodingStr := v.(string)
			if encoding, err := enumspb.EncodingTypeFromString(encodingStr); err == nil {
				eventBatch.EncodingType = enumspb.EncodingType(encoding)
			}
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
}

func updateChasmNodes(
	txn *gocql.Txn,
	chasmNodes map[string]p.InternalChasmNode,
	deleteKeys map[string]struct{},
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {
	for nodeKey, node := range chasmNodes {
		if node.CassandraBlob != nil {
			txn.Query(templateUpdateChasmNodeQuery,
				nodeKey,
				node.CassandraBlob.Data,
				node.CassandraBlob.EncodingType.String(),
				shardID,
				namespaceID,
				workflowID,
				runID)
		}
	}

	for deleteKey := range deleteKeys {
		txn.Query(templateDeleteChasmNodeQuery,
			deleteKey,
			shardID,
			namespaceID,
			workflowID,
			runID)
	}
}

func resetChasmNodes(
	txn *gocql.Txn,
	chasmNodes map[string]p.InternalChasmNode,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {
	nodeMap := make(map[string][]byte)
	var encoding enumspb.EncodingType
	for nodeKey, node := range chasmNodes {
		if node.CassandraBlob != nil {
			nodeMap[nodeKey] = node.CassandraBlob.Data
			encoding = node.CassandraBlob.EncodingType
		}
	}

	txn.Query(templateResetChasmNodesQuery,
		nodeMap,
		encoding.String(),
		shardID,
		namespaceID,
		workflowID,
		runID)
}
