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

import internalfuture "github.com/photowey/pool.go/internal/future"

// AllOf completes when every future completes successfully.
func AllOf(futures ...View) Future[struct{}] {
	return internalfuture.AllOf(futures...)
}

// All completes with all values in input order when every future succeeds.
func All[T any](futures ...Future[T]) Future[[]T] {
	return internalfuture.All(futures...)
}

// AnyOf completes with the first successful future result.
func AnyOf(futures ...View) Future[any] {
	return internalfuture.AnyOf(futures...)
}

// Any completes with the first successful future result.
func Any[T any](futures ...Future[T]) Future[T] {
	return internalfuture.Any(futures...)
}
