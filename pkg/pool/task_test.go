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

	"github.com/photowey/pool.go/pkg/pool"
)

func returnSeven(context.Context) (int, error) {
	return 7, nil
}

func markRunnable(ctx context.Context) {
	ran, ok := ctx.Value(markRunnableKey{}).(*bool)
	if ok {
		*ran = true
	}
}

type markRunnableKey struct{}

func TestFuncExecuteCallsWrappedFunction(t *testing.T) {
	value, err := pool.Func[int](returnSeven).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if value != 7 {
		t.Fatalf("value = %d, want 7", value)
	}
}

func TestFuncExecuteRejectsNilFunction(t *testing.T) {
	var fn pool.Func[int]

	_, err := fn.Execute(context.Background())
	if !errors.Is(err, pool.ErrInvalid) {
		t.Fatalf("execute error = %v, want ErrInvalid", err)
	}
}

func TestRunnableFuncRunCallsWrappedFunction(t *testing.T) {
	ran := false
	ctx := context.WithValue(context.Background(), markRunnableKey{}, &ran)

	pool.RunnableFunc(markRunnable).Run(ctx)
	if !ran {
		t.Fatal("runnable did not run")
	}
}
