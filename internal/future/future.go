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

package future

import (
	"context"
	"sync"

	"github.com/photowey/pool.go/internal/core"
)

// Awaiter waits for a future result.
type Awaiter[T any] interface {
	Await(ctx context.Context) (T, error)
	Done() <-chan struct{}
}

// ResultReader exposes a non-blocking result read.
type ResultReader[T any] interface {
	Result() (Result[T], bool)
}

// ObservableFuture supports completion observers.
type ObservableFuture[T any] interface {
	Observe(observer Observer[T]) Subscription
}

// Future is a typed read-only task result.
type Future[T any] interface {
	Awaiter[T]
	ResultReader[T]
	ObservableFuture[T]
	View
}

// View is a non-generic future view for heterogeneous composition.
type View interface {
	Done() <-chan struct{}
	ResultAny() (Result[any], bool)
	ObserveAny(observer Observer[any]) Subscription
}

type futureState[T any] struct {
	mu        sync.Mutex
	done      chan struct{}
	completed bool
	result    Result[T]
	observers map[*futureSubscription[T]]Observer[T]
}

type promiseFuture[T any] struct {
	state *futureState[T]
}

var (
	_ Future[any]           = promiseFuture[any]{}
	_ Awaiter[any]          = promiseFuture[any]{}
	_ ResultReader[any]     = promiseFuture[any]{}
	_ ObservableFuture[any] = promiseFuture[any]{}
	_ View                  = promiseFuture[any]{}
	_ Observer[any]         = anyFutureObserver[any]{}
)

func newFutureState[T any]() *futureState[T] {
	return &futureState[T]{
		done:      make(chan struct{}),
		observers: make(map[*futureSubscription[T]]Observer[T]),
	}
}

// Await waits for the future to complete or for ctx to be canceled.
func (f promiseFuture[T]) Await(ctx context.Context) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-f.state.done:
	case <-ctx.Done():
		var zero T

		return zero, ctx.Err()
	}

	result, _ := f.Result()
	if result.Err != nil {
		var zero T

		return zero, result.Err
	}
	if result.Canceled {
		var zero T

		return zero, core.ErrCanceled
	}

	return result.Value, nil
}

// Done returns a channel closed when the future completes.
func (f promiseFuture[T]) Done() <-chan struct{} {
	return f.state.done
}

// Result reads the completed result without blocking.
func (f promiseFuture[T]) Result() (Result[T], bool) {
	f.state.mu.Lock()
	defer f.state.mu.Unlock()

	if !f.state.completed {
		return Result[T]{}, false
	}

	return f.state.result, true
}

// ResultAny reads the completed result as any without blocking.
func (f promiseFuture[T]) ResultAny() (Result[any], bool) {
	result, ok := f.Result()
	if !ok {
		return Result[any]{}, false
	}

	return Result[any]{
		Value:    result.Value,
		Err:      result.Err,
		Canceled: result.Canceled,
	}, true
}

// Observe registers an observer or invokes it immediately if already complete.
func (f promiseFuture[T]) Observe(observer Observer[T]) Subscription {
	if observer == nil {
		return noopSubscription{}
	}

	subscription := &futureSubscription[T]{state: f.state}
	f.state.mu.Lock()
	if f.state.completed {
		result := f.state.result
		f.state.mu.Unlock()
		observer.OnFutureComplete(result)

		return noopSubscription{}
	}
	f.state.observers[subscription] = observer
	f.state.mu.Unlock()

	return subscription
}

// ObserveAny registers a non-generic observer.
func (f promiseFuture[T]) ObserveAny(observer Observer[any]) Subscription {
	if observer == nil {
		return noopSubscription{}
	}

	return f.Observe(anyFutureObserver[T]{observer: observer})
}

type anyFutureObserver[T any] struct {
	observer Observer[any]
}

func (observer anyFutureObserver[T]) OnFutureComplete(result Result[T]) {
	observer.observer.OnFutureComplete(Result[any]{
		Value:    result.Value,
		Err:      result.Err,
		Canceled: result.Canceled,
	})
}
