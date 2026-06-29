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

package pool

import internalfuture "github.com/photowey/pool.go/internal/future"

// Promise owns completion of a Future.
type Promise[T any] = internalfuture.Promise[T]

// NewPromise creates an incomplete promise.
func NewPromise[T any]() Promise[T] {
	return internalfuture.NewPromise[T]()
}

// Completed creates a future that already completed successfully.
func Completed[T any](value T) Future[T] {
	return internalfuture.Completed(value)
}

// Failed creates a future that already completed with err.
func Failed[T any](err error) Future[T] {
	return internalfuture.Failed[T](err)
}

// Canceled creates a future that already completed as canceled.
func Canceled[T any](cause error) Future[T] {
	return internalfuture.Canceled[T](cause)
}
