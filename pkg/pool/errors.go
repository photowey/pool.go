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

var (
	// ErrClosed reports that an executor no longer accepts tasks.
	ErrClosed = core.ErrClosed
	// ErrSaturated reports that an executor queue is full.
	ErrSaturated = core.ErrSaturated
	// ErrCanceled reports a canceled promise or submitted task.
	ErrCanceled = core.ErrCanceled
	// ErrInvalid reports invalid configuration or inputs.
	ErrInvalid = core.ErrInvalid
)
