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

// Observer receives a future completion.
type Observer[T any] interface {
	OnFutureComplete(result Result[T])
}

// ObserverFunc adapts a named function value to Observer.
type ObserverFunc[T any] func(result Result[T])

var _ Observer[any] = ObserverFunc[any](nil)

// OnFutureComplete calls the wrapped function.
func (fn ObserverFunc[T]) OnFutureComplete(result Result[T]) {
	if fn != nil {
		fn(result)
	}
}
