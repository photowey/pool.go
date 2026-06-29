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

package fixed

import (
	"fmt"
	"time"

	"github.com/photowey/pool.go/internal/core"
)

func (e *Fixed) runLoop() {
	defer e.wg.Done()
	for task := range e.tasks {
		e.runTask(task.request)
	}
}

func (e *Fixed) runTask(request core.ExecuteRequest) {
	if err := request.Context.Err(); err != nil {
		if task, ok := request.Task.(core.QueuedCancelable); ok {
			task.CancelQueued(err)
		}
		e.emitMetric("task_failed", request, err, 0)
		return
	}

	started := time.Now()
	e.emitMetric("task_started", request, nil, 0)
	defer e.recoverTaskPanic(request, started)

	request.Task.Run(request.Context)
	e.emitMetric("task_completed", request, nil, time.Since(started))
}

func (e *Fixed) recoverTaskPanic(
	request core.ExecuteRequest,
	started time.Time,
) {
	recovered := recover()
	if recovered == nil {
		return
	}

	err := fmt.Errorf("executor: task panic: %v", recovered)
	if e.config.PanicHandler != nil {
		e.config.PanicHandler.HandlePanic(core.PanicRequest{
			Context:      request.Context,
			ExecutorName: e.config.Name,
			TaskName:     request.Name,
			Recovered:    recovered,
		})
	}
	e.emitMetric("task_panicked", request, err, time.Since(started))
}

func (e *Fixed) emitMetric(
	kind string,
	request core.ExecuteRequest,
	err error,
	duration time.Duration,
) {
	if e.config.MetricsSink == nil {
		return
	}

	e.config.MetricsSink.OnMetric(core.Metric{
		ExecutorName: e.config.Name,
		TaskName:     request.Name,
		Kind:         kind,
		QueueDepth:   len(e.tasks),
		PoolSize:     e.config.Size,
		Duration:     duration,
		Err:          err,
	})
}
