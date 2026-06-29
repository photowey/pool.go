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

package benchmarks

import (
	"context"
	"testing"

	"github.com/photowey/pool.go/pkg/pool"
)

func BenchmarkFutureAwaitCompleted(b *testing.B) {
	ctx := context.Background()
	completed := pool.Completed(42)

	b.ReportAllocs()
	for b.Loop() {
		value, err := completed.Await(ctx)
		if err != nil {
			b.Fatalf("await: %v", err)
		}
		if value != 42 {
			b.Fatalf("value = %d, want 42", value)
		}
	}
}

func BenchmarkPromiseComplete(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		promise := pool.NewPromise[int]()
		if !promise.Complete(42) {
			b.Fatal("complete returned false")
		}
	}
}

func BenchmarkExecutorExecuteFixed(b *testing.B) {
	ctx := context.Background()
	executor, err := pool.NewFixed(
		2,
		pool.WithQueueSize(1024),
	)
	if err != nil {
		b.Fatalf("new fixed pool: %v", err)
	}
	b.Cleanup(benchmarkExecutorCleanup{
		b:        b,
		executor: executor,
	}.cleanup)

	done := make(chan struct{}, 1)
	task := benchmarkSignalTask{done: done}

	b.ReportAllocs()
	for b.Loop() {
		if err := executor.Execute(pool.ExecuteRequest{Context: ctx, Task: task}); err != nil {
			b.Fatalf("execute: %v", err)
		}
		<-done
	}
}

func BenchmarkExecutorSubmitFixed(b *testing.B) {
	ctx := context.Background()
	executor, err := pool.NewFixed(
		2,
		pool.WithQueueSize(1024),
	)
	if err != nil {
		b.Fatalf("new fixed pool: %v", err)
	}
	b.Cleanup(benchmarkExecutorCleanup{
		b:        b,
		executor: executor,
	}.cleanup)

	task := benchmarkIntTask{value: 42}

	b.ReportAllocs()
	for b.Loop() {
		submitted, err := pool.Submit(ctx, executor, task)
		if err != nil {
			b.Fatalf("submit: %v", err)
		}
		if _, err := submitted.Await(ctx); err != nil {
			b.Fatalf("await: %v", err)
		}
	}
}

type benchmarkSignalTask struct {
	done chan<- struct{}
}

func (task benchmarkSignalTask) Run(context.Context) {
	task.done <- struct{}{}
}

type benchmarkExecutorCleanup struct {
	b        *testing.B
	executor pool.Executor
}

func (c benchmarkExecutorCleanup) cleanup() {
	if err := c.executor.Shutdown(context.Background()); err != nil {
		c.b.Fatalf("shutdown fixed pool: %v", err)
	}
}

type benchmarkIntTask struct {
	value int
}

func (task benchmarkIntTask) Execute(context.Context) (int, error) {
	return task.value, nil
}

func BenchmarkFutureAllOf(b *testing.B) {
	first := pool.Completed(1)
	second := pool.Completed(2)

	b.ReportAllocs()
	for b.Loop() {
		values := pool.All(first, second)
		if _, err := values.Await(context.Background()); err != nil {
			b.Fatalf("await: %v", err)
		}
	}
}
