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

// SubmitOption configures typed task submission.
type SubmitOption = internalasync.SubmitOption

// SubmitConfig is the typed submission configuration.
type SubmitConfig = internalasync.SubmitConfig

// WithTaskName sets the submitted task name.
var WithTaskName = internalasync.WithTaskName
