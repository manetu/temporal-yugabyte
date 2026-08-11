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

package gocql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yugabyte/gocql"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

// TestConvertError_RequestErrWriteTimeout verifies that a write-timeout error is classified
// as persistence.TimeoutError. RequestErrWriteTimeout is always returned as a pointer by the
// driver, so the errors.As target must be *gocql.RequestErrWriteTimeout, not the value type.
func TestConvertError_RequestErrWriteTimeout(t *testing.T) {
	err := ConvertError("TestOp", &gocql.RequestErrWriteTimeout{})

	var timeoutErr *persistence.TimeoutError
	require.ErrorAs(t, err, &timeoutErr)
}

// TestConvertError_ResourceExhaustedPassthrough verifies that a *serviceerror.ResourceExhausted
// found anywhere in the error chain is returned as-is instead of being re-wrapped as Unavailable.
func TestConvertError_ResourceExhaustedPassthrough(t *testing.T) {
	original := &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		Message: "system overloaded",
	}
	wrapped := fmt.Errorf("wrapped: %w", original)

	got := ConvertError("TestOp", wrapped)

	require.Same(t, original, got)
}
