# API Guide

This guide shows the public API exposed by
`github.com/photowey/pool.go/pkg/pool`.

## Install And Import

```bash
go get github.com/photowey/pool.go
```

```go
import "github.com/photowey/pool.go/pkg/pool"
```

## Create A Fixed Executor

```go
executor, err := pool.NewFixed(
	2,
	pool.WithQueueSize(16),
	pool.WithRejectPolicy(pool.RejectPolicyReject),
	pool.WithName("orders"),
)
if err != nil {
	return err
}
defer shutdownExecutor(executor)
```

```go
func shutdownExecutor(executor pool.Executor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Shutdown(ctx); err != nil {
		panic(err)
	}
}
```

## Execute Runnable Tasks

`Executor.Execute` accepts a non-generic `Runnable`.

```go
type IndexTask struct {
	ID string
}

func (task IndexTask) Run(context.Context) {
	fmt.Println(task.ID)
}

err := executor.Execute(pool.ExecuteRequest{
	Context: context.Background(),
	Task:    IndexTask{ID: "order-1001"},
	Name:    "index-order",
})
```

Use `pool.Noop{}` when a no-operation runnable is useful in tests or control
flow.

## Submit Typed Tasks

`Submit` accepts `Executable[T]` and returns `Future[T]`.

```go
type PriceTask struct {
	Items int
}

func (task PriceTask) Execute(context.Context) (int64, error) {
	return int64(task.Items) * 599, nil
}

future, err := pool.Submit(
	context.Background(),
	executor,
	PriceTask{Items: 2},
	pool.WithTaskName("price"),
)
if err != nil {
	return err
}

total, err := future.Await(context.Background())
```

`pool.Func[T]` adapts a named function value into `Executable[T]`.

## Use Promises

`Promise[T]` is useful when a result is completed by code outside an executor
task.

```go
promise := pool.NewPromise[string]()
created := promise.Future()

go completeIndex(promise)

value, err := created.Await(context.Background())
```

```go
func completeIndex(promise pool.Promise[string]) {
	promise.Complete("order-1001-indexed")
}
```

## Aggregate Futures

`All` preserves input order and returns all values when every future succeeds.

```go
first := pool.Completed(1)
second := pool.Completed(2)

values, err := pool.All(first, second).Await(context.Background())
```

`AllOf` accepts heterogeneous `View` values and completes when every future
succeeds.

```go
_, err := pool.AllOf(receiptFuture, indexFuture).Await(context.Background())
```

`Any` and `AnyOf` complete with the first successful result.

## Compose Futures

Composition helpers register observers on parent futures and submit
continuation work to the executor supplied by the caller.

```go
type ReceiptTask struct {
	OrderID int64
}

func (task ReceiptTask) Apply(
	_ context.Context,
	steps []Step,
) (Receipt, error) {
	return Receipt{OrderID: task.OrderID}, nil
}

receiptFuture, err := pool.ThenApply(
	context.Background(),
	executor,
	stepsFuture,
	ReceiptTask{OrderID: 1001},
	pool.WithTaskName("receipt"),
)
```

Use `ThenCompose` when the continuation returns another future. Use
`Exceptionally` to recover from a failed or canceled parent future.

## Configure Backpressure

`RejectPolicyBlock` waits for queue capacity while observing the request
context.

```go
executor, err := pool.NewFixed(
	1,
	pool.WithQueueSize(1),
	pool.WithRejectPolicy(pool.RejectPolicyBlock),
)
```

`RejectPolicyReject` returns `ErrSaturated` immediately when the queue is full.

```go
if errors.Is(err, pool.ErrSaturated) {
	// caller decides whether to retry, drop, or route elsewhere
}
```

## Add Observability

Metrics sinks and panic handlers are interfaces.

```go
type MetricsSink struct{}

func (MetricsSink) OnMetric(metric pool.Metric) {
	fmt.Println(metric.Kind, metric.TaskName)
}

type PanicRecorder struct{}

func (PanicRecorder) HandlePanic(request pool.PanicRequest) {
	fmt.Println(request.TaskName, request.Recovered)
}

executor, err := pool.NewFixed(
	2,
	pool.WithMetricsSink(MetricsSink{}),
	pool.WithPanicHandler(PanicRecorder{}),
)
```

## Handle Errors

The package exports sentinel errors:

- `ErrClosed`
- `ErrSaturated`
- `ErrCanceled`
- `ErrInvalid`

Use `errors.Is` because returned errors may wrap sentinels.

```go
if errors.Is(err, pool.ErrClosed) {
	return err
}
```

## More Examples

- `pkg/pool/async_example_test.go`
- `pkg/pool/future_example_test.go`
- `examples/executor`
- `pkg/pool/*_test.go`
