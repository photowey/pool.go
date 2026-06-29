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
	"sync"
)

type allOfObserver struct {
	mu        *sync.Mutex
	remaining *int
	joined    *error
	canceled  *bool
	promise   Promise[struct{}]
}

var _ Observer[any] = allOfObserver{}

func (observer allOfObserver) OnFutureComplete(result Result[any]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if result.Canceled {
		*observer.canceled = true
	}
	if err := resultError(result); err != nil {
		*observer.joined = errors.Join(*observer.joined, err)
	}
	*observer.remaining--
	completeAllOf(
		observer.promise,
		*observer.remaining,
		*observer.joined,
		*observer.canceled,
	)
}

type allObserver[T any] struct {
	mu          *sync.Mutex
	remaining   *int
	joined      *error
	canceled    *bool
	values      []T
	resultIndex int
	promise     Promise[[]T]
}

var _ Observer[any] = allObserver[any]{}

func (observer allObserver[T]) OnFutureComplete(result Result[T]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if result.OK() {
		observer.values[observer.resultIndex] = result.Value
	}
	if result.Canceled {
		*observer.canceled = true
	}
	if err := resultError(result); err != nil {
		*observer.joined = errors.Join(*observer.joined, err)
	}
	*observer.remaining--
	if *observer.remaining > 0 {
		return
	}
	if *observer.joined != nil {
		if *observer.canceled {
			observer.promise.Cancel(*observer.joined)
			return
		}
		observer.promise.Fail(*observer.joined)
		return
	}
	observer.promise.Complete(observer.values)
}

type anyOfObserver struct {
	mu        *sync.Mutex
	remaining *int
	joined    *error
	promise   Promise[any]
}

var _ Observer[any] = anyOfObserver{}

func (observer anyOfObserver) OnFutureComplete(result Result[any]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if promiseCompleted(observer.promise.Future()) {
		return
	}
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	*observer.joined = errors.Join(*observer.joined, resultError(result))
	*observer.remaining--
	if *observer.remaining == 0 {
		completeAnyOf(observer.promise, *observer.joined)
	}
}

type anyObserver[T any] struct {
	mu        *sync.Mutex
	remaining *int
	joined    *error
	promise   Promise[T]
}

var _ Observer[any] = anyObserver[any]{}

func (observer anyObserver[T]) OnFutureComplete(result Result[T]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if promiseCompleted(observer.promise.Future()) {
		return
	}
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	*observer.joined = errors.Join(*observer.joined, resultError(result))
	*observer.remaining--
	if *observer.remaining == 0 {
		completeAny(observer.promise, *observer.joined)
	}
}
