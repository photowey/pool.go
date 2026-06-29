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

import "github.com/photowey/pool.go/internal/core"

// Runnable is the non-generic task shape used by executors.
type Runnable = core.Runnable

// QueuedCancelable receives a cancellation cause before a queued task runs.
type QueuedCancelable = core.QueuedCancelable

// RunnableFunc adapts a named function value to Runnable.
type RunnableFunc = core.RunnableFunc

// Noop is a runnable task that does nothing.
type Noop = core.Noop
