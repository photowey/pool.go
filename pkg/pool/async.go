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

package pool

import (
	"context"

	internalasync "github.com/photowey/pool.go/internal/async"
)

// Submit submits a typed task to an executor and returns its typed future.
func Submit[T any](
	ctx context.Context,
	exec Executor,
	work Executable[T],
	opts ...SubmitOption,
) (Future[T], error) {
	return internalasync.Submit(ctx, exec, work, opts...)
}

// ThenApply runs task on executor after parent completes successfully.
func ThenApply[T, R any](
	ctx context.Context,
	exec Executor,
	parent Future[T],
	work ApplyTask[T, R],
	opts ...SubmitOption,
) (Future[R], error) {
	return internalasync.ThenApply(ctx, exec, parent, work, opts...)
}

// ThenCompose runs task on executor after parent succeeds and flattens its future.
func ThenCompose[T, R any](
	ctx context.Context,
	exec Executor,
	parent Future[T],
	work ComposeTask[T, R],
	opts ...SubmitOption,
) (Future[R], error) {
	return internalasync.ThenCompose(ctx, exec, parent, work, opts...)
}

// Exceptionally runs task on executor after parent fails.
func Exceptionally[T any](
	ctx context.Context,
	exec Executor,
	parent Future[T],
	work RecoverTask[T],
	opts ...SubmitOption,
) (Future[T], error) {
	return internalasync.Exceptionally(ctx, exec, parent, work, opts...)
}
