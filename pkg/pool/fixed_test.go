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

type fixedBlockingTask struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (t fixedBlockingTask) Run(context.Context) {
	close(t.started)
	<-t.release
}

type fixedCanceledTask struct {
	canceled chan<- error
}

func (t fixedCanceledTask) Run(context.Context) {}

func (t fixedCanceledTask) CancelQueued(cause error) {
	t.canceled <- cause
}

type fixedPanicTask struct{}

func (fixedPanicTask) Run(context.Context) {
	panic("boom")
}

type fixedMetricRecorder struct {
	metrics chan pool.Metric
}

func (r fixedMetricRecorder) OnMetric(metric pool.Metric) {
	r.metrics <- metric
}

type fixedPanicRecorder struct {
	requests chan pool.PanicRequest
}

func (r fixedPanicRecorder) HandlePanic(request pool.PanicRequest) {
	r.requests <- request
}

func TestFixedRejectsWhenSaturated(t *testing.T) {
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(1),
		pool.WithRejectPolicy(pool.RejectPolicyReject),
	)
	if err != nil {
		t.Fatalf("new fixed: %v", err)
	}
	defer shutdownFixed(t, executor)

	release := make(chan struct{})
	started := make(chan struct{})
	if err := executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task: fixedBlockingTask{
			started: started,
			release: release,
		},
	}); err != nil {
		t.Fatalf("execute blocking: %v", err)
	}
	<-started

	if err := executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task:    pool.Noop{},
	}); err != nil {
		close(release)
		t.Fatalf("execute queued: %v", err)
	}

	err = executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task:    pool.Noop{},
	})
	close(release)
	if !errors.Is(err, pool.ErrSaturated) {
		t.Fatalf("execute error = %v, want ErrSaturated", err)
	}
}

func TestFixedCancelsQueuedTaskBeforeRun(t *testing.T) {
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(1),
		pool.WithRejectPolicy(pool.RejectPolicyReject),
	)
	if err != nil {
		t.Fatalf("new fixed: %v", err)
	}
	defer shutdownFixed(t, executor)

	release := make(chan struct{})
	started := make(chan struct{})
	if err := executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task: fixedBlockingTask{
			started: started,
			release: release,
		},
	}); err != nil {
		t.Fatalf("execute blocking: %v", err)
	}
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	if err := executor.Execute(pool.ExecuteRequest{
		Context: ctx,
		Task:    fixedCanceledTask{canceled: canceled},
	}); err != nil {
		close(release)
		t.Fatalf("execute queued: %v", err)
	}

	cancel()
	close(release)
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancel cause = %v, want context.Canceled", err)
	}
}

func TestFixedEmitsMetricsAndPanicRequest(t *testing.T) {
	metricsSink := fixedMetricRecorder{metrics: make(chan pool.Metric, 8)}
	panicHandler := fixedPanicRecorder{requests: make(chan pool.PanicRequest, 1)}
	executor, err := pool.NewFixed(
		1,
		pool.WithQueueSize(1),
		pool.WithName("panic-pool"),
		pool.WithMetricsSink(metricsSink),
		pool.WithPanicHandler(panicHandler),
	)
	if err != nil {
		t.Fatalf("new fixed: %v", err)
	}
	defer shutdownFixed(t, executor)

	err = executor.Execute(pool.ExecuteRequest{
		Context: context.Background(),
		Task:    fixedPanicTask{},
		Name:    "panic-task",
	})
	if err != nil {
		t.Fatalf("execute panic task: %v", err)
	}

	request := <-panicHandler.requests
	if request.ExecutorName != "panic-pool" {
		t.Fatalf("executor name = %q, want panic-pool", request.ExecutorName)
	}
	if request.TaskName != "panic-task" {
		t.Fatalf("task name = %q, want panic-task", request.TaskName)
	}

	if !hasMetricKind(metricsSink.metrics, "task_panicked") {
		t.Fatal("task_panicked metric not observed")
	}
}

func shutdownFixed(t *testing.T, executor pool.Executor) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func hasMetricKind(metrics <-chan pool.Metric, kind string) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for {
		select {
		case metric := <-metrics:
			if metric.Kind == kind {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}
