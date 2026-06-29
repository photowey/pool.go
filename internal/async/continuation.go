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

package async

import (
	"context"
	"fmt"

	"github.com/photowey/pool.go/internal/core"
	"github.com/photowey/pool.go/internal/future"
)

// ThenApply runs task on executor after parent completes successfully.
func ThenApply[T, R any](
	ctx context.Context,
	exec core.Executor,
	parent future.Future[T],
	work ApplyTask[T, R],
	opts ...SubmitOption,
) (future.Future[R], error) {
	if parent == nil {
		return nil, fmt.Errorf("%w: parent future is nil", core.ErrInvalid)
	}

	promise := future.NewPromise[R]()
	parent.Observe(thenApplyObserver[T, R]{
		ctx:     ctx,
		exec:    exec,
		work:    work,
		opts:    opts,
		promise: promise,
	})

	return promise.Future(), nil
}

// ThenCompose runs task on executor after parent succeeds and flattens its future.
func ThenCompose[T, R any](
	ctx context.Context,
	exec core.Executor,
	parent future.Future[T],
	work ComposeTask[T, R],
	opts ...SubmitOption,
) (future.Future[R], error) {
	if parent == nil {
		return nil, fmt.Errorf("%w: parent future is nil", core.ErrInvalid)
	}

	promise := future.NewPromise[R]()
	parent.Observe(thenComposeObserver[T, R]{
		ctx:     ctx,
		exec:    exec,
		work:    work,
		opts:    opts,
		promise: promise,
	})

	return promise.Future(), nil
}

// Exceptionally runs task on executor after parent fails.
func Exceptionally[T any](
	ctx context.Context,
	exec core.Executor,
	parent future.Future[T],
	work RecoverTask[T],
	opts ...SubmitOption,
) (future.Future[T], error) {
	if parent == nil {
		return nil, fmt.Errorf("%w: parent future is nil", core.ErrInvalid)
	}

	promise := future.NewPromise[T]()
	parent.Observe(exceptionallyObserver[T]{
		ctx:     ctx,
		exec:    exec,
		work:    work,
		opts:    opts,
		promise: promise,
	})

	return promise.Future(), nil
}

func chainFuture[T any](
	promise future.Promise[T],
	next future.Future[T],
	err error,
) {
	if err != nil {
		promise.Fail(err)
		return
	}
	if next == nil {
		promise.Fail(fmt.Errorf("%w: future is nil", core.ErrInvalid))
		return
	}
	next.Observe(chainFutureObserver[T]{promise: promise})
}

func completeFromResult[T, R any](
	promise future.Promise[R],
	result future.Result[T],
) {
	if result.Canceled {
		promise.Cancel(resultError(result))
		return
	}
	promise.Fail(resultError(result))
}

func resultError[T any](result future.Result[T]) error {
	if result.Canceled && result.Err == nil {
		return core.ErrCanceled
	}
	if result.Err == nil && !result.OK() {
		return core.ErrInvalid
	}

	return result.Err
}
