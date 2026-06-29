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
	"errors"
	"testing"
	"time"

	"github.com/photowey/pool.go/pkg/pool"
)

type intValueTask struct {
	value int
}

func (task intValueTask) Execute(context.Context) (int, error) {
	return task.value, nil
}

type blockingRunnableTask struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (task blockingRunnableTask) Run(context.Context) {
	close(task.started)
	<-task.release
}

type recoverCanceledTask struct {
	value int
}

func (task recoverCanceledTask) Recover(_ context.Context, err error) (int, error) {
	if !errors.Is(err, pool.ErrCanceled) {
		return 0, err
	}

	return task.value, nil
}

type addValueTask struct {
	delta int
}

func (task addValueTask) Apply(_ context.Context, value int) (int, error) {
	return value + task.delta, nil
}

type stringFutureTask struct {
	text string
}

func (task stringFutureTask) Compose(
	context.Context,
	int,
) (pool.Future[string], error) {
	return pool.Completed(task.text), nil
}

type recoverValueTask struct {
	value int
}

func (task recoverValueTask) Recover(context.Context, error) (int, error) {
	return task.value, nil
}

func TestSubmitCompletesTypedFuture(t *testing.T) {
	ctx := context.Background()
	executor := newTestExecutor(t)
	task := intValueTask{value: 7}

	submitted, err := pool.Submit(ctx, executor, task, pool.WithTaskName("seven"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	value, err := submitted.Await(ctx)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if value != 7 {
		t.Fatalf("value = %d, want 7", value)
	}
}

func TestSubmitRejectsNilExecutorAndTask(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Submit[int](ctx, nil, intValueTask{}); !errors.Is(err, pool.ErrInvalid) {
		t.Fatalf("nil executor error = %v, want ErrInvalid", err)
	}

	executor := newTestExecutor(t)
	if _, err := pool.Submit[int](ctx, executor, nil); !errors.Is(err, pool.ErrInvalid) {
		t.Fatalf("nil task error = %v, want ErrInvalid", err)
	}
}

func TestFixedExecutorRejectsWhenSaturated(t *testing.T) {
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(1),
		pool.WithRejectPolicy(pool.RejectPolicyReject),
	)
	if err != nil {
		t.Fatalf("new fixed pool: %v", err)
	}
	defer shutdownExecutor(t, executor)

	release := make(chan struct{})
	started := make(chan struct{})
	blocking := blockingRunnableTask{
		started: started,
		release: release,
	}
	if err := executor.Execute(pool.ExecuteRequest{Context: context.Background(), Task: blocking}); err != nil {
		t.Fatalf("execute blocking: %v", err)
	}
	<-started
	if err := executor.Execute(pool.ExecuteRequest{Context: context.Background(), Task: pool.Noop{}}); err != nil {
		t.Fatalf("execute queued: %v", err)
	}
	if err := executor.Execute(pool.ExecuteRequest{Context: context.Background(), Task: pool.Noop{}}); !errors.Is(err, pool.ErrSaturated) {
		close(release)
		t.Fatalf("saturated execute error = %v, want ErrSaturated", err)
	}
	close(release)
}

func TestFixedExecutorBlockPolicyRespectsContext(t *testing.T) {
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(1),
		pool.WithRejectPolicy(pool.RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("new fixed pool: %v", err)
	}
	defer shutdownExecutor(t, executor)

	release := make(chan struct{})
	started := make(chan struct{})
	blocking := blockingRunnableTask{
		started: started,
		release: release,
	}
	if err := executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task:    blocking,
	}); err != nil {
		t.Fatalf("execute blocking: %v", err)
	}
	<-started
	if err := executor.Execute(pool.ExecuteRequest{Context: context.Background(), Task: pool.Noop{}}); err != nil {
		t.Fatalf("execute queued: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := executor.Execute(pool.ExecuteRequest{Context: ctx, Task: pool.Noop{}}); !errors.Is(err, context.Canceled) {
		close(release)
		t.Fatalf("blocked execute error = %v, want context.Canceled", err)
	}
	close(release)
}

func TestSubmittedFutureCancelsWhenQueuedContextIsCanceled(t *testing.T) {
	ctx := context.Background()
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(1),
		pool.WithRejectPolicy(pool.RejectPolicyReject),
	)
	if err != nil {
		t.Fatalf("new fixed pool: %v", err)
	}
	defer shutdownExecutor(t, executor)

	release := make(chan struct{})
	started := make(chan struct{})
	blocking := blockingRunnableTask{
		started: started,
		release: release,
	}
	if err := executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task:    blocking,
	}); err != nil {
		t.Fatalf("execute blocking: %v", err)
	}
	<-started

	queuedCtx, cancel := context.WithCancel(context.Background())
	submitted, err := pool.Submit(
		queuedCtx,
		executor,
		intValueTask{value: 42},
	)
	if err != nil {
		close(release)
		t.Fatalf("submit queued typed task: %v", err)
	}
	cancel()
	close(release)

	_, err = submitted.Await(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await queued canceled future error = %v, want context.Canceled", err)
	}
}

func TestFixedExecutorShutdownStopsAcceptingTasks(t *testing.T) {
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(1),
	)
	if err != nil {
		t.Fatalf("new fixed pool: %v", err)
	}
	if err := executor.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := executor.Execute(pool.ExecuteRequest{Context: context.Background(), Task: pool.Noop{}}); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("execute after shutdown error = %v, want ErrClosed", err)
	}
}

func TestFixedExecutorShutdownUnblocksPendingExecute(t *testing.T) {
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(0),
		pool.WithRejectPolicy(pool.RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("new fixed pool: %v", err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	blocking := blockingRunnableTask{
		started: started,
		release: release,
	}
	if err := executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task:    blocking,
	}); err != nil {
		t.Fatalf("execute blocking: %v", err)
	}
	<-started

	executeErr := make(chan error, 1)
	go executeNoopTask(executor, executeErr)
	waitForPendingExecute(t, executeErr)

	shutdownErr := make(chan error, 1)
	go shutdownExecutorAsync(executor, shutdownErr)
	if err := <-executeErr; !errors.Is(err, pool.ErrClosed) {
		close(release)
		t.Fatalf("pending execute error = %v, want ErrClosed", err)
	}
	close(release)
	if err := <-shutdownErr; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestExceptionallyHandlesCanceledFuture(t *testing.T) {
	ctx := context.Background()
	executor := newTestExecutor(t)
	canceled := asyncCompletedView[int]{
		result: pool.Result[int]{Canceled: true},
	}

	recovered, err := pool.Exceptionally(
		ctx,
		executor,
		canceled,
		recoverCanceledTask{value: 9},
	)
	if err != nil {
		t.Fatalf("exceptionally canceled: %v", err)
	}
	value, err := recovered.Await(ctx)
	if err != nil {
		t.Fatalf("await recovered canceled: %v", err)
	}
	if value != 9 {
		t.Fatalf("recovered canceled value = %d, want 9", value)
	}
}

func TestThenApplyThenComposeAndExceptionally(t *testing.T) {
	ctx := context.Background()
	executor := newTestExecutor(t)
	base := pool.Completed(10)

	applied, err := pool.ThenApply(
		ctx,
		executor,
		base,
		addValueTask{delta: 5},
	)
	if err != nil {
		t.Fatalf("then apply: %v", err)
	}
	value, err := applied.Await(ctx)
	if err != nil {
		t.Fatalf("await applied: %v", err)
	}
	if value != 15 {
		t.Fatalf("applied value = %d, want 15", value)
	}

	composed, err := pool.ThenCompose(
		ctx,
		executor,
		applied,
		stringFutureTask{text: "value"},
	)
	if err != nil {
		t.Fatalf("then compose: %v", err)
	}
	text, err := composed.Await(ctx)
	if err != nil {
		t.Fatalf("await composed: %v", err)
	}
	if text != "value" {
		t.Fatalf("composed value = %q, want value", text)
	}

	failed := pool.Failed[int](errors.New("boom"))
	recovered, err := pool.Exceptionally(
		ctx,
		executor,
		failed,
		recoverValueTask{value: 99},
	)
	if err != nil {
		t.Fatalf("exceptionally: %v", err)
	}
	got, err := recovered.Await(ctx)
	if err != nil {
		t.Fatalf("await recovered: %v", err)
	}
	if got != 99 {
		t.Fatalf("recovered = %d, want 99", got)
	}
}

func newTestExecutor(t *testing.T) pool.Executor {
	t.Helper()

	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(8),
	)
	if err != nil {
		t.Fatalf("new fixed pool: %v", err)
	}
	t.Cleanup(executorCleanup{t: t, executor: executor}.run)

	return executor
}

type executorCleanup struct {
	t        *testing.T
	executor pool.Executor
}

func (cleanup executorCleanup) run() {
	shutdownExecutor(cleanup.t, cleanup.executor)
}

func shutdownExecutor(t *testing.T, executor pool.Executor) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown executor: %v", err)
	}
}

func executeNoopTask(exec pool.Executor, executeErr chan<- error) {
	executeErr <- exec.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task:    pool.Noop{},
	})
}

func shutdownExecutorAsync(exec pool.Executor, shutdownErr chan<- error) {
	shutdownErr <- exec.Shutdown(context.Background())
}

func waitForPendingExecute(t *testing.T, executeErr <-chan error) {
	t.Helper()

	select {
	case err := <-executeErr:
		t.Fatalf("execute returned before shutdown: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

type asyncCompletedView[T any] struct {
	result pool.Result[T]
	done   chan struct{}
}

func (f asyncCompletedView[T]) Await(context.Context) (T, error) {
	if f.result.OK() {
		return f.result.Value, nil
	}
	if f.result.Canceled {
		var zero T

		if f.result.Err != nil {
			return zero, f.result.Err
		}

		return zero, pool.ErrCanceled
	}
	var zero T

	return zero, f.result.Err
}

func (f asyncCompletedView[T]) Done() <-chan struct{} {
	if f.done != nil {
		return f.done
	}

	done := make(chan struct{})
	close(done)

	return done
}

func (f asyncCompletedView[T]) Result() (pool.Result[T], bool) {
	return f.result, true
}

func (f asyncCompletedView[T]) Observe(observer pool.Observer[T]) pool.Subscription {
	if observer != nil {
		observer.OnFutureComplete(f.result)
	}

	return completedSubscription{}
}

func (f asyncCompletedView[T]) ResultAny() (pool.Result[any], bool) {
	return pool.Result[any]{
		Value:    f.result.Value,
		Err:      f.result.Err,
		Canceled: f.result.Canceled,
	}, true
}

func (f asyncCompletedView[T]) ObserveAny(
	observer pool.Observer[any],
) pool.Subscription {
	if observer != nil {
		result, _ := f.ResultAny()
		observer.OnFutureComplete(result)
	}

	return asyncCompletedSubscription{}
}

type asyncCompletedSubscription struct{}

func (asyncCompletedSubscription) Unsubscribe() bool {
	return false
}
