# pool.go

[![Go Reference](https://pkg.go.dev/badge/github.com/photowey/pool.go/pkg/pool.svg)](https://pkg.go.dev/github.com/photowey/pool.go/pkg/pool)
[![Go Version](https://img.shields.io/github/go-mod/go-version/photowey/pool.go)](https://go.dev/)
[![License](https://img.shields.io/github/license/photowey/pool.go)](./LICENSE)

`pool.go` provides bounded task execution, typed futures, producer-owned
promises, and explicit asynchronous composition for Go applications.

The library uses one public facade package. Applications import `pkg/pool`,
choose the executor implementation, configure queue policy and observability,
and compose asynchronous work on caller-owned executors.

## Install

```bash
go get github.com/photowey/pool.go
```

Import the public API package:

```go
import "github.com/photowey/pool.go/pkg/pool"
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/photowey/pool.go/pkg/pool"
)

type DoubleTask struct {
	Value int
}

func (task DoubleTask) Execute(context.Context) (int, error) {
	return task.Value * 2, nil
}

func main() {
	ctx := context.Background()
	executor, err := pool.NewFixed(
		2,
		pool.WithQueueSize(16),
		pool.WithRejectPolicy(pool.RejectPolicyReject),
		pool.WithName("example"),
	)
	if err != nil {
		panic(err)
	}
	defer shutdown(executor)

	submitted, err := pool.Submit(
		ctx,
		executor,
		DoubleTask{Value: 21},
		pool.WithTaskName("double"),
	)
	if err != nil {
		panic(err)
	}

	value, err := submitted.Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}

func shutdown(executor pool.Executor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Shutdown(ctx); err != nil {
		panic(err)
	}
}
```

## Documentation

- [Design](./docs/design.md): goals, API surface, execution model, future model,
  observability, errors, and benchmark scope.
- [API Guide](./docs/api-guide.md): practical usage for executors, typed tasks,
  promises, aggregation, composition, backpressure, and metrics hooks.
- [Architecture](./docs/architecture.md): system architecture, package
  structure, dependency direction, request flow, typed submit flow, lifecycle,
  futures, composition, observability, and CI.

## Package Layout

Public API:

- `pkg/pool`: executor contracts, fixed pool constructor, options, runnable and
  typed task contracts, metrics hooks, typed futures, promises, aggregation, and
  async composition helpers.

Private implementation packages:

- `internal/core`: shared contracts and sentinel errors.
- `internal/fixed`: fixed-size bounded execution implementation.
- `internal/future`: Future/Promise implementation and aggregation internals.
- `internal/async`: typed submission and continuation implementation.

## API Shape

```go
type Executor interface {
	Execute(request ExecuteRequest) error
	Shutdown(ctx context.Context) error
}
```

```go
type Future[T any] interface {
	Await(ctx context.Context) (T, error)
	Done() <-chan struct{}
	Result() (Result[T], bool)
	Observe(observer Observer[T]) Subscription
	ResultAny() (Result[any], bool)
	ObserveAny(observer Observer[any]) Subscription
}

type Promise[T any] interface {
	Future() Future[T]
	Complete(value T) bool
	Fail(err error) bool
	Cancel(cause error) bool
}
```

Core building blocks:

- `pool.Executor.Execute`: fire-and-forget runnable task execution.
- `pool.NewFixed`: bounded fixed-size execution pool.
- `pool.Submit`: typed task submission that returns a typed future.
- `pool.NewPromise`: producer-owned completion handle.
- `pool.Completed`, `pool.Failed`, `pool.Canceled`: ready future helpers.
- `pool.All`, `pool.AllOf`, `pool.Any`, `pool.AnyOf`: future aggregation.
- `pool.ThenApply`, `pool.ThenCompose`, `pool.Exceptionally`: explicit
  continuation helpers.

## Backpressure

`pool.NewFixed` accepts a pool size, queue size, and reject policy:

- `RejectPolicyBlock`: wait for queue capacity while observing context
  cancellation.
- `RejectPolicyReject`: return `ErrSaturated` immediately when the queue is
  full.

Queue size is explicit. A zero-size queue creates direct handoff between
submitters and execution loops.

## Observability

Executors can receive a metrics sink and a panic handler:

```go
executor, err := pool.NewFixed(
	2,
	pool.WithMetricsSink(mySink),
	pool.WithPanicHandler(myPanicHandler),
)
```

Metrics are backend-neutral payloads. The library does not choose a telemetry
backend.

## Examples

- `pkg/pool/future_example_test.go`: Future and Promise examples for pkg.go.dev.
- `pkg/pool/async_example_test.go`: typed submission and continuation examples.
- `examples/executor`: runnable order workflow using pools, futures, promises,
  and composition.
- `pkg/pool/future_test.go`: executable Future and Promise specification.
- `pkg/pool/async_test.go`: executable async submission specification.

Run the standalone example:

```bash
go run ./examples/executor
```

## Benchmarks

Benchmark smoke test:

```bash
go test -run '^$' -bench=. -benchmem -benchtime=100ms -count=1 ./benchmarks
```

Local benchmark suite:

```bash
make bench
```

## Development

```bash
make ci
```

The CI target runs formatting checks, tests with shuffle and coverage, race
tests, `go vet`, `golangci-lint`, and a benchmark smoke test.

## License

Apache License 2.0. See [LICENSE](./LICENSE).
