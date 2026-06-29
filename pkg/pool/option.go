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

// RejectPolicy determines fixed pool backpressure behavior.
type RejectPolicy = core.RejectPolicy

const (
	// RejectPolicyBlock waits for queue capacity while observing context.
	RejectPolicyBlock = core.RejectPolicyBlock
	// RejectPolicyReject returns ErrSaturated when the queue is full.
	RejectPolicyReject = core.RejectPolicyReject
)

// Option configures a fixed pool.
type Option = core.Option

// Config is the validated fixed pool configuration.
type Config = core.Config

// WithQueueSize sets the bounded queue size.
var WithQueueSize = core.WithQueueSize

// WithName sets the executor name.
var WithName = core.WithName

// WithRejectPolicy sets queue saturation behavior.
var WithRejectPolicy = core.WithRejectPolicy

// WithPanicHandler sets a recovered panic observer.
var WithPanicHandler = core.WithPanicHandler

// WithMetricsSink sets an executor metrics sink.
var WithMetricsSink = core.WithMetricsSink
