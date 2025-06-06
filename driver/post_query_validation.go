// The MIT License
//
// Copyright (c) 2025 Manetu Inc.  All rights reserved.
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
	"sync"
)

type (
	PostQueryValidation struct {
		validators []func() error
	}
)

func NewPostQueryValidation() *PostQueryValidation {
	return &PostQueryValidation{
		validators: []func() error{},
	}
}

func (p *PostQueryValidation) Add(validator func() error) {
	p.validators = append(p.validators, validator)
}

func (d *PostQueryValidation) Validate() error {
	results := make(chan error, len(d.validators))
	wg := sync.WaitGroup{}
	wg.Add(len(d.validators))

	for _, validator := range d.validators {
		go func() {
			results <- validator()
			wg.Done()
		}()
	}

	wg.Wait()
	close(results)

	var errors []error
	for result := range results {
		if result != nil {
			errors = append(errors, result)
		}
	}

	if len(errors) > 0 {
		return sortErrors(errors)[0]
	}

	return nil
}
