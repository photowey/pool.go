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

package future

import "sync"

// Subscription removes an observer registration.
type Subscription interface {
	Unsubscribe() bool
}

type futureSubscription[T any] struct {
	once      sync.Once
	state     *futureState[T]
	isRemoved bool
}

var (
	_ Subscription = (*futureSubscription[any])(nil)
	_ Subscription = noopSubscription{}
)

func (s *futureSubscription[T]) Unsubscribe() bool {
	s.once.Do(s.unsubscribe)

	return s.isRemoved
}

func (s *futureSubscription[T]) unsubscribe() {
	s.isRemoved = true
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.observers == nil {
		return
	}
	delete(s.state.observers, s)
}

type noopSubscription struct{}

func (noopSubscription) Unsubscribe() bool {
	return false
}
