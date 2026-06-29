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

import (
	"context"

	internalfuture "github.com/photowey/pool.go/internal/future"
)

// Awaiter waits for a future result.
type Awaiter[T any] = internalfuture.Awaiter[T]

// ResultReader exposes a non-blocking result read.
type ResultReader[T any] = internalfuture.ResultReader[T]

// ObservableFuture supports completion observers.
type ObservableFuture[T any] = internalfuture.ObservableFuture[T]

// Future is a typed read-only task result.
type Future[T any] = internalfuture.Future[T]

// View is a non-generic future view for heterogeneous composition.
type View = internalfuture.View

// Result is the immutable completion value of a future.
type Result[T any] = internalfuture.Result[T]

// Observer receives a future completion.
type Observer[T any] = internalfuture.Observer[T]

// ObserverFunc adapts a named function value to Observer.
type ObserverFunc[T any] = internalfuture.ObserverFunc[T]

// Subscription removes an observer registration.
type Subscription = internalfuture.Subscription

// Await waits for future to complete or for ctx to be canceled.
func Await[T any](ctx context.Context, item Future[T]) (T, error) {
	return item.Await(ctx)
}
