using Amazon.SQS;
using Orders.Core;
using Orders.Host.Listeners;

// One-shot demo commands, handled before the host is built.
if (args is ["send", ..])
{
    await Publisher.SendAsync(args);
    return;
}

if (args is ["dlq", ..])
{
    await Publisher.PeekDeadLetterAsync();
    return;
}

if (args is ["reset", ..])
{
    await Publisher.ResetAsync();
    return;
}

var builder = Host.CreateApplicationBuilder(args);

// ─────────────────────────────────────────────────────────────────────
// The handler is registered once, the same way, for both modes.
// Nothing below this line is allowed to change it.
// ─────────────────────────────────────────────────────────────────────
builder.Services.AddSingleton<IOrderHandler, OrderHandler>();

// ─────────────────────────────────────────────────────────────────────
// Only the listener swaps. Set "Listener" to Timer or Sqs.
// ─────────────────────────────────────────────────────────────────────
var listener = builder.Configuration["Listener"] ?? "Timer";

if (listener.Equals("Sqs", StringComparison.OrdinalIgnoreCase))
{
    builder.Services.AddSingleton(Sqs.CreateClient(builder.Configuration));
    builder.Services.AddHostedService<SqsListener>();
}
else
{
    builder.Services.AddSingleton<PendingOrders>();
    builder.Services.AddHostedService<TimerListener>();
}

Console.WriteLine($"=== Listener: {listener} ===");

builder.Build().Run();
