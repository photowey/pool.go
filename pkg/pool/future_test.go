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
	"sync"
	"testing"
	"time"

	"github.com/photowey/pool.go/pkg/pool"
)

func TestPromiseCompletesExactlyOnce(t *testing.T) {
	promise := pool.NewPromise[int]()

	const attempts = 32
	var wg sync.WaitGroup
	wg.Add(attempts)
	for index := 0; index < attempts; index++ {
		value := index
		task := promiseCompleteTask{
			wg:      &wg,
			promise: promise,
			value:   value,
		}
		go task.run()
	}
	wg.Wait()

	result, ok := promise.Future().Result()
	if !ok {
		t.Fatal("future is not complete")
	}
	if !result.OK() {
		t.Fatalf("result OK = false, err=%v canceled=%v", result.Err, result.Canceled)
	}
	if promise.Complete(100) {
		t.Fatal("second complete succeeded")
	}
	if promise.Fail(errors.New("late failure")) {
		t.Fatal("late failure succeeded")
	}
}

func TestPromiseAwaitRespectsContext(t *testing.T) {
	promise := pool.NewPromise[int]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := promise.Future().Await(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
}

func TestFutureObserversRunForLateAndEarlySubscribers(t *testing.T) {
	promise := pool.NewPromise[int]()
	early := make(chan int, 1)
	late := make(chan int, 1)

	promise.Future().Observe(valueObserver{values: early})
	if !promise.Complete(42) {
		t.Fatal("complete returned false")
	}
	promise.Future().Observe(valueObserver{values: late})

	wantObserverValue(t, early, 42)
	wantObserverValue(t, late, 42)
}

func TestAllAndAnyComposition(t *testing.T) {
	ctx := context.Background()
	first := pool.Completed(1)
	second := pool.Completed(2)

	values, err := pool.All(first, second).Await(ctx)
	if err != nil {
		t.Fatalf("all await: %v", err)
	}
	if len(values) != 2 || values[0] != 1 || values[1] != 2 {
		t.Fatalf("values = %v, want [1 2]", values)
	}

	value, err := pool.Any(first, second).Await(ctx)
	if err != nil {
		t.Fatalf("any await: %v", err)
	}
	if value != 1 && value != 2 {
		t.Fatalf("any value = %d, want 1 or 2", value)
	}
}

func TestCompositionHandlesConcurrentCompletion(t *testing.T) {
	ctx := context.Background()

	for attempt := 0; attempt < 100; attempt++ {
		first := pool.NewPromise[int]()
		second := pool.NewPromise[int]()
		start := make(chan struct{})
		go completePromiseAfterStart(start, first, 1)
		go completePromiseAfterStart(start, second, 2)
		close(start)

		values, err := pool.All(first.Future(), second.Future()).Await(ctx)
		if err != nil {
			t.Fatalf("all await attempt %d: %v", attempt, err)
		}
		if len(values) != 2 || values[0] != 1 || values[1] != 2 {
			t.Fatalf("values attempt %d = %v, want [1 2]", attempt, values)
		}

		anyFirst := pool.NewPromise[int]()
		anySecond := pool.NewPromise[int]()
		anyStart := make(chan struct{})
		go completePromiseAfterStart(anyStart, anyFirst, 1)
		go completePromiseAfterStart(anyStart, anySecond, 2)
		close(anyStart)

		value, err := pool.Any(anyFirst.Future(), anySecond.Future()).Await(ctx)
		if err != nil {
			t.Fatalf("any await attempt %d: %v", attempt, err)
		}
		if value != 1 && value != 2 {
			t.Fatalf("any value attempt %d = %d, want 1 or 2", attempt, value)
		}
	}
}

func TestCompositionNormalizesCustomCanceledResults(t *testing.T) {
	ctx := context.Background()
	canceled := completedView[int]{
		result: pool.Result[int]{Canceled: true},
	}
	succeeded := pool.Completed(7)

	_, err := pool.All(canceled).Await(ctx)
	if !errors.Is(err, pool.ErrCanceled) {
		t.Fatalf("all canceled error = %v, want ErrCanceled", err)
	}

	_, err = pool.AllOf(canceled).Await(ctx)
	if !errors.Is(err, pool.ErrCanceled) {
		t.Fatalf("allOf canceled error = %v, want ErrCanceled", err)
	}

	value, err := pool.Any(canceled, succeeded).Await(ctx)
	if err != nil {
		t.Fatalf("any canceled then succeeded: %v", err)
	}
	if value != 7 {
		t.Fatalf("any value = %d, want 7", value)
	}

	_, err = pool.Any(canceled).Await(ctx)
	if !errors.Is(err, pool.ErrCanceled) {
		t.Fatalf("any canceled error = %v, want ErrCanceled", err)
	}

	_, err = pool.AnyOf(canceled).Await(ctx)
	if !errors.Is(err, pool.ErrCanceled) {
		t.Fatalf("anyOf canceled error = %v, want ErrCanceled", err)
	}
}

type promiseCompleteTask struct {
	wg      *sync.WaitGroup
	promise pool.Promise[int]
	value   int
}

func (task promiseCompleteTask) run() {
	defer task.wg.Done()
	_ = task.promise.Complete(task.value)
}

type valueObserver struct {
	values chan<- int
}

func (observer valueObserver) OnFutureComplete(result pool.Result[int]) {
	observer.values <- result.Value
}

func wantObserverValue(t *testing.T, values <-chan int, want int) {
	t.Helper()

	select {
	case got := <-values:
		if got != want {
			t.Fatalf("observer value = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observer")
	}
}

func completePromiseAfterStart[T any](
	start <-chan struct{},
	promise pool.Promise[T],
	value T,
) {
	<-start
	promise.Complete(value)
}

type completedView[T any] struct {
	result pool.Result[T]
	done   chan struct{}
}

func (f completedView[T]) Await(context.Context) (T, error) {
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

func (f completedView[T]) Done() <-chan struct{} {
	if f.done != nil {
		return f.done
	}

	done := make(chan struct{})
	close(done)

	return done
}

func (f completedView[T]) Result() (pool.Result[T], bool) {
	return f.result, true
}

func (f completedView[T]) Observe(observer pool.Observer[T]) pool.Subscription {
	if observer != nil {
		observer.OnFutureComplete(f.result)
	}

	return completedSubscription{}
}

func (f completedView[T]) ResultAny() (pool.Result[any], bool) {
	return pool.Result[any]{
		Value:    f.result.Value,
		Err:      f.result.Err,
		Canceled: f.result.Canceled,
	}, true
}

func (f completedView[T]) ObserveAny(
	observer pool.Observer[any],
) pool.Subscription {
	if observer != nil {
		result, _ := f.ResultAny()
		observer.OnFutureComplete(result)
	}

	return completedSubscription{}
}

type completedSubscription struct{}

func (completedSubscription) Unsubscribe() bool {
	return false
}
