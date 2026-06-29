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
	"fmt"
	"time"

	"github.com/photowey/pool.go/pkg/pool"
)

type exampleOrder struct {
	id    int64
	items []string
}

type exampleOrderStep struct {
	name  string
	value int64
}

type exampleValidateTask struct {
	order exampleOrder
}

func (t exampleValidateTask) Execute(context.Context) (exampleOrderStep, error) {
	if len(t.order.items) == 0 {
		return exampleOrderStep{}, fmt.Errorf("order %d has no items", t.order.id)
	}

	return exampleOrderStep{name: "validate", value: 1}, nil
}

type examplePriceTask struct {
	order exampleOrder
}

func (t examplePriceTask) Execute(context.Context) (exampleOrderStep, error) {
	return exampleOrderStep{
		name:  "price",
		value: int64(len(t.order.items)) * 599,
	}, nil
}

type exampleReceiptTask struct {
	orderID int64
}

func (t exampleReceiptTask) Apply(
	_ context.Context,
	steps []exampleOrderStep,
) (string, error) {
	var total int64
	for _, step := range steps {
		if step.name == "price" {
			total = step.value
		}
	}

	return fmt.Sprintf("order-%d:%d", t.orderID, total), nil
}

func ExampleSubmit_orderWorkflow() {
	ctx := context.Background()
	executor, err := pool.NewFixed(
		2,
		pool.WithQueueSize(2),
		pool.WithRejectPolicy(pool.RejectPolicyReject),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownExampleExecutor(executor)

	order := exampleOrder{
		id:    1001,
		items: []string{"book", "pen"},
	}
	validateFuture, err := pool.Submit(
		ctx,
		executor,
		exampleValidateTask{order: order},
		pool.WithTaskName("validate"),
	)
	if err != nil {
		panic(err)
	}
	priceFuture, err := pool.Submit(
		ctx,
		executor,
		examplePriceTask{order: order},
		pool.WithTaskName("price"),
	)
	if err != nil {
		panic(err)
	}

	stepsFuture := pool.All(validateFuture, priceFuture)
	receiptFuture, err := pool.ThenApply(
		ctx,
		executor,
		stepsFuture,
		exampleReceiptTask{orderID: order.id},
	)
	if err != nil {
		panic(err)
	}
	receipt, err := receiptFuture.Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(receipt)

	// Output: order-1001:1198
}

func shutdownExampleExecutor(pool pool.Executor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Shutdown(ctx); err != nil {
		panic(err)
	}
}
