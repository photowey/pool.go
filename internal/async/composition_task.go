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

package async

import (
	"context"
	"fmt"

	"github.com/photowey/pool.go/internal/core"
	"github.com/photowey/pool.go/internal/future"
)

// ApplyTask maps a parent future value to a new value.
type ApplyTask[T, R any] interface {
	Apply(ctx context.Context, value T) (R, error)
}

// ApplyTaskFunc adapts a named function value to ApplyTask.
type ApplyTaskFunc[T, R any] func(ctx context.Context, value T) (R, error)

var _ ApplyTask[any, any] = ApplyTaskFunc[any, any](nil)

// Apply calls the wrapped function.
func (fn ApplyTaskFunc[T, R]) Apply(ctx context.Context, value T) (R, error) {
	if fn == nil {
		var zero R

		return zero, fmt.Errorf("%w: apply task func is nil", core.ErrInvalid)
	}

	return fn(ctx, value)
}

// ComposeTask maps a parent future value to another future.
type ComposeTask[T, R any] interface {
	Compose(ctx context.Context, value T) (future.Future[R], error)
}

// ComposeTaskFunc adapts a named function value to ComposeTask.
type ComposeTaskFunc[T, R any] func(
	ctx context.Context,
	value T,
) (future.Future[R], error)

var _ ComposeTask[any, any] = ComposeTaskFunc[any, any](nil)

// Compose calls the wrapped function.
func (fn ComposeTaskFunc[T, R]) Compose(
	ctx context.Context,
	value T,
) (future.Future[R], error) {
	if fn == nil {
		return nil, fmt.Errorf("%w: compose task func is nil", core.ErrInvalid)
	}

	return fn(ctx, value)
}

// RecoverTask maps a parent future error to a replacement value.
type RecoverTask[T any] interface {
	Recover(ctx context.Context, err error) (T, error)
}

// RecoverTaskFunc adapts a named function value to RecoverTask.
type RecoverTaskFunc[T any] func(ctx context.Context, err error) (T, error)

var _ RecoverTask[any] = RecoverTaskFunc[any](nil)

// Recover calls the wrapped function.
func (fn RecoverTaskFunc[T]) Recover(ctx context.Context, err error) (T, error) {
	if fn == nil {
		var zero T

		return zero, fmt.Errorf("%w: recover task func is nil", core.ErrInvalid)
	}

	return fn(ctx, err)
}
