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
	"encoding/json"
	"fmt"
	"github.com/manetu/temporal-yugabyte/utils/gocql"
	"math"
	"reflect"
	"sort"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

var (
	errorPriority = map[reflect.Type]int{
		reflect.TypeOf(&p.ShardOwnershipLostError{}):             0,
		reflect.TypeOf(&p.CurrentWorkflowConditionFailedError{}): 1,
		reflect.TypeOf(&p.WorkflowConditionFailedError{}):        2,
		reflect.TypeOf(&p.ConditionFailedError{}):                3,
	}

	errorDefaultPriority = math.MaxInt64
)

type (
	executionCASCondition struct {
		runID       string
		dbVersion   int64
		nextEventID int64 // TODO deprecate this variable once DB version comparison is the default
	}
)

// ScyllaDB will return rows with null values to match # of queries in a batch query (see #2683).
// To support null values, fields type should be a pointer to pointer of underlying type (i.e. **int).
// Resulting value will be converted to a pointer of underlying type (i.e. *int) and stored in the map.
// We do it only for "type" field which is checked for `nil` value.
// All other fields are created automatically by gocql with non-pointer types (i.e. int).
func newConflictRecord() map[string]interface{} {
	t := new(int)
	return map[string]interface{}{
		"type": &t,
	}
}

func sortErrors(
	errors []error,
) []error {
	sort.Slice(errors, func(i int, j int) bool {
		leftPriority, ok := errorPriority[reflect.TypeOf(errors[i])]
		if !ok {
			leftPriority = errorDefaultPriority
		}
		rightPriority, ok := errorPriority[reflect.TypeOf(errors[j])]
		if !ok {
			rightPriority = errorDefaultPriority
		}
		return leftPriority < rightPriority
	})
	return errors
}

func extractShardOwnershipLostError(
	conflictRecord map[string]interface{},
	requestShardID int32,
	requestRangeID int64,
) error {
	actualRangeID := conflictRecord["range_id"].(int64)
	if actualRangeID != requestRangeID {
		return &p.ShardOwnershipLostError{
			ShardID: requestShardID,
			Msg: fmt.Sprintf("Encounter shard ownership lost, request range ID: %v, actual range ID: %v",
				requestRangeID,
				actualRangeID,
			),
		}
	}
	return nil
}

func exportCurrentWorkflowConflictError(
	conflictRecord map[string]interface{},
) error {
	actualCurrentRunID := gocql.UUIDToString(conflictRecord["current_run_id"])
	binary, _ := conflictRecord["execution_state"].([]byte)
	encoding, _ := conflictRecord["execution_state_encoding"].(string)
	executionState := &persistencespb.WorkflowExecutionState{}
	if state, err := serialization.WorkflowExecutionStateFromBlob(
		p.NewDataBlob(binary, encoding),
	); err == nil {
		executionState = state
	}
	// if err != nil, this means execution state cannot be parsed, just use default values

	lastWriteVersion, _ := conflictRecord["workflow_last_write_version"].(int64)

	// TODO maybe assert actualCurrentRunID == executionState.RunId ?

	return &p.CurrentWorkflowConditionFailedError{
		Msg: fmt.Sprintf("Encounter current workflow conflict run ID: %v",
			actualCurrentRunID,
		),
		RequestIDs:       executionState.RequestIds,
		RunID:            executionState.RunId,
		State:            executionState.State,
		Status:           executionState.Status,
		LastWriteVersion: lastWriteVersion,
		StartTime:        timestamp.TimeValuePtr(executionState.StartTime),
	}
}

func extractWorkflowConflictError(
	conflictRecord map[string]interface{},
	requestDBVersion int64,
	requestNextEventID int64, // TODO deprecate this variable once DB version comparison is the default
) error {
	actualNextEventID, _ := conflictRecord["next_event_id"].(int64)
	actualDBVersion, _ := conflictRecord["db_record_version"].(int64)

	// TODO remove this block once DB version comparison is the default
	if requestDBVersion == 0 {
		if actualNextEventID != requestNextEventID {
			return &p.WorkflowConditionFailedError{
				Msg: fmt.Sprintf("Encounter workflow next event ID mismatch, request next event ID: %v, actual next event ID: %v",
					requestNextEventID,
					actualNextEventID,
				),
				NextEventID:     actualNextEventID,
				DBRecordVersion: actualDBVersion,
			}
		}
		return nil
	}

	if actualDBVersion != requestDBVersion {
		return &p.WorkflowConditionFailedError{
			Msg: fmt.Sprintf("Encounter workflow db version mismatch, request db version: %v, actual db version: %v",
				requestDBVersion,
				actualDBVersion,
			),
			NextEventID:     actualNextEventID,
			DBRecordVersion: actualDBVersion,
		}
	}
	return nil
}

func printRecords(
	records []map[string]interface{},
) string {
	binary, _ := json.MarshalIndent(records, "", "  ")
	return string(binary)
}
