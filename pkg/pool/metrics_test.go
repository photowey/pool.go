// Copyright © 2026-present The Pool.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pool_test

import (
	"context"
	"testing"

	"github.com/photowey/pool.go/pkg/pool"
)

func recordPanicRequest(request pool.PanicRequest) {
	requests := request.Context.Value(panicRequestsKey{}).(chan pool.PanicRequest)
	requests <- request
}

type panicRequestsKey struct{}

func TestPanicHandlerFuncHandlesRequest(t *testing.T) {
	requests := make(chan pool.PanicRequest, 1)
	request := pool.PanicRequest{
		Context: contextWithPanicRequests(requests),
	}

	pool.PanicHandlerFunc(recordPanicRequest).HandlePanic(request)

	got := <-requests
	if got.Context != request.Context {
		t.Fatal("panic request context mismatch")
	}
}

func contextWithPanicRequests(requests chan pool.PanicRequest) context.Context {
	return context.WithValue(context.Background(), panicRequestsKey{}, requests)
}
