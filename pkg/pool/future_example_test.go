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

	"github.com/photowey/pool.go/pkg/pool"
)

type exampleExternalProducer struct {
	value string
}

func (p exampleExternalProducer) Complete(promise pool.Promise[string]) {
	promise.Complete(p.value)
}

func ExampleNewPromise() {
	ctx := context.Background()
	promise := pool.NewPromise[string]()
	created := promise.Future()

	producer := exampleExternalProducer{value: "indexed"}
	go producer.Complete(promise)

	value, err := created.Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)

	// Output: indexed
}

func ExampleAll() {
	ctx := context.Background()
	first := pool.Completed(1)
	second := pool.Completed(2)

	values, err := pool.All(first, second).Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(values)

	// Output: [1 2]
}
