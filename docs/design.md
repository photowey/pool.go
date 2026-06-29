# Design

`pool.go` is a small Go library for bounded execution, typed futures,
producer-owned promises, and executor-backed asynchronous composition.

## Goals

- Expose one stable public package: `github.com/photowey/pool.go/pkg/pool`.
- Keep runtime behavior explicit: callers choose the executor, queue policy,
  shutdown deadline, metrics sink, and panic handler.
- Program to interfaces: task execution, continuation work, metrics, panic
  handling, futures, and promises are all expressed as replaceable contracts.
- Run asynchronous composition on caller-owned executors. Continuation work uses
  the executor supplied by the caller.
- Preserve Go idioms: functional options configure constructors, context values
  carry cancellation, and examples are executable tests.
- Keep implementation packages private under root `internal/`.

## Boundaries

- The public import surface is `github.com/photowey/pool.go/pkg/pool`.
- Runtime policy remains caller-owned: retry, scheduling, timeout policy beyond
  context handling, and telemetry backend selection belong to application code.
- Implementation packages stay private under root `internal/`.
- Continuation execution is explicit and always uses a caller-supplied
  executor.

## Public Surface

The public surface is the `pkg/pool` facade:

- Execution: `Executor`, `ExecuteRequest`, `Runnable`, `QueuedCancelable`,
  `Noop`, `NewFixed`.
- Configuration: `Option`, `WithQueueSize`, `WithRejectPolicy`, `WithName`,
  `WithMetricsSink`, `WithPanicHandler`.
- Typed work: `Executable[T]`, `Func[T]`.
- Futures and promises: `Future[T]`, `Promise[T]`, `Result[T]`,
  `NewPromise`, `Completed`, `Failed`, `Canceled`.
- Aggregation: `All`, `AllOf`, `Any`, `AnyOf`.
- Composition: `Submit`, `ThenApply`, `ThenCompose`, `Exceptionally`,
  `ApplyTask`, `ComposeTask`, `RecoverTask`.
- Observability: `Sink`, `Metric`, `PanicHandler`, `PanicRequest`.

## Execution Model

`pool.NewFixed(size, opts...)` creates a bounded fixed-size executor. The
constructor requires a positive size. Queue size is explicit:

- `WithQueueSize(0)` creates direct handoff.
- `WithQueueSize(n)` creates a bounded queue with capacity `n`.

`RejectPolicyBlock` waits for queue capacity while observing the request
context. `RejectPolicyReject` returns `ErrSaturated` when the queue is full.

`Execute` accepts `ExecuteRequest` values. A nil request context is normalized
to `context.Background()`. A nil task returns `ErrInvalid`. Shutdown stops new
submissions, closes the task queue, and waits for execution loops to exit or for
the shutdown context to finish.

## Future Model

`Promise[T]` is the producer-owned completion handle. `Future[T]` is the
read-only result view.

A promise completes exactly once:

- `Complete(value)` stores a successful value.
- `Fail(err)` stores an error.
- `Cancel(cause)` stores a canceled result.

`Future.Await(ctx)` returns the value, completion error, cancellation error, or
the waiting context error. Observers registered before completion run when the
future completes. Observers registered after completion run immediately.

## Composition Model

`Submit` adapts an `Executable[T]` into a runnable task, submits it to an
executor, and returns `Future[T]`.

`ThenApply`, `ThenCompose`, and `Exceptionally` register observers on a parent
future and submit continuation work to the executor supplied by the caller.
Continuation execution is therefore bounded by the same executor and queue
policy chosen by the application.

## Observability

Executors emit backend-neutral `Metric` values through `Sink`. Metrics include
executor name, task name, event kind, queue depth, pool size, duration, and
error. Task panics are recovered by the executor and reported to
`PanicHandler` when configured.

## Error Model

Sentinel errors are exported from `pkg/pool`:

- `ErrClosed`: executor no longer accepts tasks.
- `ErrSaturated`: bounded queue is full under reject policy.
- `ErrCanceled`: promise or queued submitted task was canceled.
- `ErrInvalid`: invalid configuration or input.

Returned errors may wrap these sentinels. Callers should use `errors.Is`.

## Testing And Benchmarks

Public behavior is specified through `pkg/pool` tests and examples. The test
suite includes goroutine leak detection through `go.uber.org/goleak`, race tests
in CI, runnable examples, and benchmark smoke tests.

Benchmarks cover completed future await, direct fixed executor execution, typed
submit on a fixed executor, promise completion, and future aggregation.
