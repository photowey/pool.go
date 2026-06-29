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
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/photowey/pool.go/internal/core"
)

// Fixed runs tasks on a bounded goroutine pool.
type Fixed struct {
	config core.Config
	tasks  chan workItem

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	executeMu sync.RWMutex
	closed    atomic.Bool
}

var _ core.Executor = (*Fixed)(nil)

type workItem struct {
	request core.ExecuteRequest
}

type shutdownWaitTask struct {
	wg   *sync.WaitGroup
	done chan<- struct{}
}

func (t shutdownWaitTask) run() {
	t.wg.Wait()
	close(t.done)
}

// NewFixed creates and starts a fixed-size goroutine pool.
func NewFixed(size int, opts ...core.Option) (*Fixed, error) {
	config := core.Config{
		Size:         size,
		QueueSize:    0,
		Name:         "pool",
		RejectPolicy: core.RejectPolicyBlock,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&config); err != nil {
			return nil, fmt.Errorf("applying fixed pool option: %w", err)
		}
	}
	if config.Size < 1 {
		return nil, fmt.Errorf("%w: pool size must be positive", core.ErrInvalid)
	}
	if config.QueueSize < 0 {
		return nil, fmt.Errorf("%w: queue size is negative", core.ErrInvalid)
	}

	ctx, cancel := context.WithCancel(context.Background())
	exec := &Fixed{
		config: config,
		tasks:  make(chan workItem, config.QueueSize),
		ctx:    ctx,
		cancel: cancel,
	}
	exec.wg.Add(config.Size)
	for range config.Size {
		go exec.runLoop()
	}

	return exec, nil
}

// Execute runs a task according to the configured reject policy.
func (e *Fixed) Execute(request core.ExecuteRequest) error {
	if request.Context == nil {
		request.Context = context.Background()
	}
	if err := request.Context.Err(); err != nil {
		return err
	}
	if request.Task == nil {
		return fmt.Errorf("%w: runnable task is nil", core.ErrInvalid)
	}
	if e.closed.Load() {
		return core.ErrClosed
	}

	e.executeMu.RLock()
	defer e.executeMu.RUnlock()
	if e.closed.Load() {
		return core.ErrClosed
	}

	task := workItem{request: request}
	switch e.config.RejectPolicy {
	case core.RejectPolicyReject:
		select {
		case e.tasks <- task:
			e.emitMetric("task_accepted", request, nil, 0)
			return nil
		default:
			e.emitMetric("task_rejected", request, core.ErrSaturated, 0)
			return core.ErrSaturated
		}
	default:
		select {
		case e.tasks <- task:
			e.emitMetric("task_accepted", request, nil, 0)
			return nil
		case <-request.Context.Done():
			err := request.Context.Err()
			e.emitMetric("task_rejected", request, err, 0)
			return err
		case <-e.ctx.Done():
			return core.ErrClosed
		}
	}
}

// Shutdown stops accepting tasks and waits for execution loops to exit.
func (e *Fixed) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.closed.CompareAndSwap(false, true) {
		e.cancel()
		e.executeMu.Lock()
		close(e.tasks)
		e.executeMu.Unlock()
	}

	done := make(chan struct{})
	task := shutdownWaitTask{
		wg:   &e.wg,
		done: done,
	}
	go task.run()

	select {
	case <-done:
		e.emitMetric("executor_shutdown", core.ExecuteRequest{}, nil, 0)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
