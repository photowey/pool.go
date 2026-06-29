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
	"errors"
	"fmt"
	"sync"

	"github.com/photowey/pool.go/internal/core"
)

// AllOf completes when every future completes successfully.
func AllOf(futures ...View) Future[struct{}] {
	promise := NewPromise[struct{}]()
	if len(futures) == 0 {
		promise.Complete(struct{}{})
		return promise.Future()
	}

	active := make([]View, 0, len(futures))
	var joined error
	for _, item := range futures {
		if item == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", core.ErrInvalid))
			continue
		}
		active = append(active, item)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	var canceled bool
	for _, item := range active {
		item.ObserveAny(allOfObserver{
			mu:        &mu,
			remaining: &remaining,
			joined:    &joined,
			canceled:  &canceled,
			promise:   promise,
		})
	}

	return promise.Future()
}

func completeAllOf(
	promise Promise[struct{}],
	remaining int,
	err error,
	canceled bool,
) {
	if remaining > 0 {
		return
	}
	if err != nil {
		if canceled {
			promise.Cancel(err)
			return
		}
		promise.Fail(err)
		return
	}
	promise.Complete(struct{}{})
}

// All completes with all values in input order when every future succeeds.
func All[T any](futures ...Future[T]) Future[[]T] {
	promise := NewPromise[[]T]()
	if len(futures) == 0 {
		promise.Complete([]T{})
		return promise.Future()
	}

	active := make([]Future[T], 0, len(futures))
	indexes := make([]int, 0, len(futures))
	values := make([]T, len(futures))
	var joined error
	for index, item := range futures {
		if item == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", core.ErrInvalid))
			continue
		}
		active = append(active, item)
		indexes = append(indexes, index)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	var canceled bool
	for activeIndex, item := range active {
		resultIndex := indexes[activeIndex]
		item.Observe(allObserver[T]{
			mu:          &mu,
			remaining:   &remaining,
			joined:      &joined,
			canceled:    &canceled,
			values:      values,
			resultIndex: resultIndex,
			promise:     promise,
		})
	}

	return promise.Future()
}

// AnyOf completes with the first successful future result.
func AnyOf(futures ...View) Future[any] {
	promise := NewPromise[any]()
	if len(futures) == 0 {
		promise.Fail(fmt.Errorf("%w: no futures", core.ErrInvalid))
		return promise.Future()
	}

	active := make([]View, 0, len(futures))
	var joined error
	for _, item := range futures {
		if item == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", core.ErrInvalid))
			continue
		}
		active = append(active, item)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	for _, item := range active {
		item.ObserveAny(anyOfObserver{
			mu:        &mu,
			remaining: &remaining,
			joined:    &joined,
			promise:   promise,
		})
	}

	return promise.Future()
}

// Any completes with the first successful future result.
func Any[T any](futures ...Future[T]) Future[T] {
	promise := NewPromise[T]()
	if len(futures) == 0 {
		promise.Fail(fmt.Errorf("%w: no futures", core.ErrInvalid))
		return promise.Future()
	}

	active := make([]Future[T], 0, len(futures))
	var joined error
	for _, item := range futures {
		if item == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", core.ErrInvalid))
			continue
		}
		active = append(active, item)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	for _, item := range active {
		item.Observe(anyObserver[T]{
			mu:        &mu,
			remaining: &remaining,
			joined:    &joined,
			promise:   promise,
		})
	}

	return promise.Future()
}

func promiseCompleted[T any](future Future[T]) bool {
	_, ok := future.Result()

	return ok
}

func completeAnyOf(promise Promise[any], err error) {
	if errors.Is(err, core.ErrCanceled) {
		promise.Cancel(err)
		return
	}
	promise.Fail(err)
}

func completeAny[T any](promise Promise[T], err error) {
	if errors.Is(err, core.ErrCanceled) {
		promise.Cancel(err)
		return
	}
	promise.Fail(err)
}

func resultError[T any](result Result[T]) error {
	if result.Canceled && result.Err == nil {
		return core.ErrCanceled
	}
	if result.Err == nil && !result.OK() {
		return core.ErrInvalid
	}

	return result.Err
}
