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

import internalasync "github.com/photowey/pool.go/internal/async"

// ApplyTask maps a parent future value to a new value.
type ApplyTask[T, R any] = internalasync.ApplyTask[T, R]

// ApplyTaskFunc adapts a named function value to ApplyTask.
type ApplyTaskFunc[T, R any] = internalasync.ApplyTaskFunc[T, R]

// ComposeTask maps a parent future value to another future.
type ComposeTask[T, R any] = internalasync.ComposeTask[T, R]

// ComposeTaskFunc adapts a named function value to ComposeTask.
type ComposeTaskFunc[T, R any] = internalasync.ComposeTaskFunc[T, R]

// RecoverTask maps a parent future error to a replacement value.
type RecoverTask[T any] = internalasync.RecoverTask[T]

// RecoverTaskFunc adapts a named function value to RecoverTask.
type RecoverTaskFunc[T any] = internalasync.RecoverTaskFunc[T]
