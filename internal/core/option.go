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

package core

import (
	"fmt"
	"strings"
)

type RejectPolicy uint8

const (
	RejectPolicyBlock RejectPolicy = iota + 1
	RejectPolicyReject
)

type Option func(config *Config) error

type Config struct {
	Size         int
	QueueSize    int
	Name         string
	RejectPolicy RejectPolicy
	PanicHandler PanicHandler
	MetricsSink  Sink
}

func WithQueueSize(size int) Option {
	return func(config *Config) error {
		if size < 0 {
			return fmt.Errorf("%w: queue size is negative", ErrInvalid)
		}
		config.QueueSize = size
		return nil
	}
}

func WithName(name string) Option {
	return func(config *Config) error {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("%w: executor name is empty", ErrInvalid)
		}
		config.Name = name
		return nil
	}
}

func WithRejectPolicy(policy RejectPolicy) Option {
	return func(config *Config) error {
		switch policy {
		case RejectPolicyBlock, RejectPolicyReject:
			config.RejectPolicy = policy
			return nil
		default:
			return fmt.Errorf("%w: invalid reject policy", ErrInvalid)
		}
	}
}

func WithPanicHandler(handler PanicHandler) Option {
	return func(config *Config) error {
		config.PanicHandler = handler
		return nil
	}
}

func WithMetricsSink(sink Sink) Option {
	return func(config *Config) error {
		config.MetricsSink = sink
		return nil
	}
}
