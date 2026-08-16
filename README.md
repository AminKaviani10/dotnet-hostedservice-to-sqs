# From .NET HostedService to AWS SQS

A polling `BackgroundService` and an SQS consumer, running the same business logic.

The point of the sample is what *doesn't* change. `OrderHandler` is identical in
both modes — it has no idea whether a timer or a queue delivered the work. Only
the listener differs, and switching between them is a one-line config change.

```
src/Orders.Core/            shared module — no AWS SDK, no hosting types
  OrderPlaced.cs              the message
  IOrderHandler.cs            the seam every listener depends on
  OrderHandler.cs             the business logic

src/Orders.Host/Listeners/  the adapters — only these differ
  TimerListener.cs            polls on a PeriodicTimer
  SqsListener.cs              long-polls SQS
  PendingOrders.cs            stands in for a "WHERE Processed = 0" table
  Sqs.cs                      client, queue, and redrive policy setup
  Publisher.cs                send / dlq / reset commands
```

`Program.cs` registers the handler once, then picks a listener from the `Listener`
configuration value.

## Requirements

- .NET 10 SDK
- Docker, for SQS mode only. Timer mode needs nothing.

## Running

Timer mode — no dependencies:

```bash
dotnet run --project src/Orders.Host -- --Listener=Timer
```

Five orders are queued in memory and processed two per tick. One of them
(`ORD-004`) has a negative amount and is rejected by the handler; watch it return
on every subsequent tick, indefinitely. That is the problem SQS solves.

SQS mode, against LocalStack:

```bash
docker compose up -d                                       # wait for (healthy)

dotnet run --project src/Orders.Host -- --Listener=SQS     # terminal 1
dotnet run --project src/Orders.Host -- send Alice 42      # terminal 2
```

The order is processed on arrival rather than on the next tick. Then send one the
handler rejects:

```bash
dotnet run --project src/Orders.Host -- send Dave -5
dotnet run --project src/Orders.Host -- dlq       # after ~30s
```

It is attempted three times, then SQS moves it to the dead-letter queue on its
own. Compare that with `ORD-004` looping forever in timer mode.

`dotnet run --project src/Orders.Host -- reset` empties both queues.

The application creates its own queue, DLQ, and redrive policy at startup so no
AWS CLI setup is needed. In a real system that belongs in Terraform or CDK.

> LocalStack is pinned to `4.0` in `docker-compose.yml`. The `latest` tag on that
> image is licence-gated and exits with a license activation error.

## The interesting comparison

Both listeners call `IOrderHandler` and both catch exceptions from it. The
difference is what happens next.

`TimerListener` has to decide by itself. Put the order back and it retries every
tick forever; drop it and the order is silently lost. Doing it properly means a
retry counter, somewhere to park failures, and row locking once more than one
instance is running — all of it hand-written infrastructure.

`SqsListener` does nothing at all:

```csharp
catch (Exception ex)
{
    _logger.LogError(ex, "Order {OrderId} failed — leaving it for redelivery", order.OrderId);
}
```

Not deleting the message *is* the rejection. It reappears after the visibility
timeout, and the redrive policy dead-letters it after `maxReceiveCount` attempts.
The error handling got smaller, not bigger.

## Settings worth knowing

Configured in `Sqs.cs`. Two differ from AWS defaults so behaviour is quick to
observe:

| Setting | Value | Note |
|---|---|---|
| `WaitTimeSeconds` | 20s | long poll |
| `VisibilityTimeout` | 10s | AWS default is 30s; must exceed handler runtime in production |
| `maxReceiveCount` | 3 | then the message goes to the DLQ |
| Client `Timeout` | 30s | must stay above `WaitTimeSeconds`, or long polls die with `TimeoutException` |
| Timer interval | 5s | usually minutes in production |

## Not covered

- **Idempotency.** SQS delivers at least once, so duplicate delivery is normal
  rather than a bug. A handler with real side effects needs a dedupe check.
- **EventBridge.** A bus and rule in front of the queue for fan-out. It wraps the
  payload in a `detail` field, so the listener would unwrap before deserializing.
- **FIFO queues, batching, `MessageGroupId`.**
