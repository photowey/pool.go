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

	"github.com/photowey/pool.go/internal/core"
	"github.com/photowey/pool.go/internal/future"
)

type thenApplyObserver[T, R any] struct {
	ctx     context.Context
	exec    core.Executor
	work    ApplyTask[T, R]
	opts    []SubmitOption
	promise future.Promise[R]
}

var _ future.Observer[any] = thenApplyObserver[any, any]{}

func (observer thenApplyObserver[T, R]) OnFutureComplete(result future.Result[T]) {
	if !result.OK() {
		completeFromResult(observer.promise, result)
		return
	}
	next, err := Submit(
		observer.ctx,
		observer.exec,
		thenApplyTask[T, R]{
			work:  observer.work,
			value: result.Value,
		},
		observer.opts...,
	)
	chainFuture(observer.promise, next, err)
}

type thenApplyTask[T, R any] struct {
	work  ApplyTask[T, R]
	value T
}

var _ core.Executable[any] = thenApplyTask[any, any]{}

func (t thenApplyTask[T, R]) Execute(ctx context.Context) (R, error) {
	return t.work.Apply(ctx, t.value)
}

type thenComposeObserver[T, R any] struct {
	ctx     context.Context
	exec    core.Executor
	work    ComposeTask[T, R]
	opts    []SubmitOption
	promise future.Promise[R]
}

var _ future.Observer[any] = thenComposeObserver[any, any]{}

func (observer thenComposeObserver[T, R]) OnFutureComplete(result future.Result[T]) {
	if !result.OK() {
		completeFromResult(observer.promise, result)
		return
	}
	next, err := Submit(
		observer.ctx,
		observer.exec,
		thenComposeTask[T, R]{
			work:  observer.work,
			value: result.Value,
		},
		observer.opts...,
	)
	if err != nil {
		observer.promise.Fail(err)
		return
	}
	next.Observe(thenComposeFutureObserver[R]{promise: observer.promise})
}

type thenComposeTask[T, R any] struct {
	work  ComposeTask[T, R]
	value T
}

var _ core.Executable[future.Future[any]] = thenComposeTask[any, any]{}

func (t thenComposeTask[T, R]) Execute(
	ctx context.Context,
) (future.Future[R], error) {
	return t.work.Compose(ctx, t.value)
}

type thenComposeFutureObserver[T any] struct {
	promise future.Promise[T]
}

var _ future.Observer[future.Future[any]] = thenComposeFutureObserver[any]{}

func (observer thenComposeFutureObserver[T]) OnFutureComplete(
	result future.Result[future.Future[T]],
) {
	if !result.OK() {
		completeFromResult(observer.promise, result)
		return
	}
	chainFuture(observer.promise, result.Value, nil)
}

type exceptionallyObserver[T any] struct {
	ctx     context.Context
	exec    core.Executor
	work    RecoverTask[T]
	opts    []SubmitOption
	promise future.Promise[T]
}

var _ future.Observer[any] = exceptionallyObserver[any]{}

func (observer exceptionallyObserver[T]) OnFutureComplete(result future.Result[T]) {
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	next, err := Submit(
		observer.ctx,
		observer.exec,
		recoverTaskExecution[T]{
			work: observer.work,
			err:  resultError(result),
		},
		observer.opts...,
	)
	chainFuture(observer.promise, next, err)
}

type recoverTaskExecution[T any] struct {
	work RecoverTask[T]
	err  error
}

var _ core.Executable[any] = recoverTaskExecution[any]{}

func (t recoverTaskExecution[T]) Execute(ctx context.Context) (T, error) {
	return t.work.Recover(ctx, t.err)
}

type chainFutureObserver[T any] struct {
	promise future.Promise[T]
}

var _ future.Observer[any] = chainFutureObserver[any]{}

func (observer chainFutureObserver[T]) OnFutureComplete(result future.Result[T]) {
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	if result.Canceled {
		observer.promise.Cancel(resultError(result))
		return
	}
	observer.promise.Fail(resultError(result))
}
