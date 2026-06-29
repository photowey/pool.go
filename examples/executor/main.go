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

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/photowey/pool.go/pkg/pool"
)

type order struct {
	id       int64
	items    []string
	customer string
}

type stepResult struct {
	name  string
	value int64
}

type receipt struct {
	orderID int64
	status  string
	total   int64
}

type validateOrderTask struct {
	order order
}

func (t validateOrderTask) Execute(context.Context) (stepResult, error) {
	if len(t.order.items) == 0 {
		return stepResult{}, fmt.Errorf("order %d has no items", t.order.id)
	}
	if strings.TrimSpace(t.order.customer) == "" {
		return stepResult{}, fmt.Errorf("order %d has no customer", t.order.id)
	}

	return stepResult{name: "validate", value: 1}, nil
}

type priceOrderTask struct {
	order order
}

func (t priceOrderTask) Execute(context.Context) (stepResult, error) {
	return stepResult{
		name:  "price",
		value: int64(len(t.order.items))*599 + 599,
	}, nil
}

type reserveInventoryTask struct {
	order order
}

func (t reserveInventoryTask) Execute(context.Context) (stepResult, error) {
	return stepResult{
		name:  "reserve",
		value: int64(len(t.order.items)),
	}, nil
}

type receiptTask struct {
	orderID int64
}

func (t receiptTask) Apply(_ context.Context, steps []stepResult) (receipt, error) {
	status := "ready"
	var total int64
	for _, step := range steps {
		if step.name == "price" {
			total = step.value
		}
	}

	return receipt{
		orderID: t.orderID,
		status:  status,
		total:   total,
	}, nil
}

type externalPromiseProducer struct {
	orderID int64
}

func (p externalPromiseProducer) CompleteAfter(
	promise pool.Promise[string],
	delay time.Duration,
) {
	time.Sleep(delay)
	promise.Complete(fmt.Sprintf("order-%d-indexed", p.orderID))
}

func main() {
	ctx := context.Background()
	executor, err := pool.NewFixed(
		2,
		pool.WithQueueSize(4),
		pool.WithRejectPolicy(pool.RejectPolicyReject),
		pool.WithName("orders"),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownExecutor(executor)

	current := order{
		id:       1001,
		items:    []string{"book", "pen"},
		customer: "Ada",
	}

	validateFuture := mustSubmit(ctx, executor, validateOrderTask{order: current})
	priceFuture := mustSubmit(ctx, executor, priceOrderTask{order: current})
	reserveFuture := mustSubmit(ctx, executor, reserveInventoryTask{order: current})
	stepsFuture := pool.All(validateFuture, priceFuture, reserveFuture)
	receiptFuture, err := pool.ThenApply(
		ctx,
		executor,
		stepsFuture,
		receiptTask{orderID: current.id},
		pool.WithTaskName("build-receipt"),
	)
	if err != nil {
		panic(err)
	}

	indexedPromise := pool.NewPromise[string]()
	producer := externalPromiseProducer{orderID: current.id}
	go producer.CompleteAfter(indexedPromise, time.Millisecond)

	indexFuture := indexedPromise.Future()
	if _, err := pool.AllOf(receiptFuture, indexFuture).Await(ctx); err != nil {
		panic(err)
	}
	createdReceipt, err := receiptFuture.Await(ctx)
	if err != nil {
		panic(err)
	}
	indexStatus, err := indexFuture.Await(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"order=%d status=%s index=%s receipt=order-%d:%s:%d\n",
		current.id,
		createdReceipt.status,
		indexStatus,
		createdReceipt.orderID,
		createdReceipt.status,
		createdReceipt.total,
	)
}

func mustSubmit[T any](
	ctx context.Context,
	executor pool.Executor,
	work pool.Executable[T],
) pool.Future[T] {
	future, err := pool.Submit(ctx, executor, work)
	if err != nil {
		panic(err)
	}

	return future
}

func shutdownExecutor(executor pool.Executor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Shutdown(ctx); err != nil {
		panic(err)
	}
}
