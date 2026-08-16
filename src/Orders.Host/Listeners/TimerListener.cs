using Orders.Core;

namespace Orders.Host.Listeners;

/// <summary>
/// BEFORE: the listener you already have.
///
/// It wakes up on an interval, goes looking for work, and calls the handler.
/// Everything about it is your code — including every failure decision.
/// </summary>
public class TimerListener : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);

    private readonly PendingOrders _pending;
    private readonly IOrderHandler _handler;
    private readonly ILogger<TimerListener> _logger;

    public TimerListener(
        PendingOrders pending,
        IOrderHandler handler,
        ILogger<TimerListener> logger)
    {
        _pending = pending;
        _handler = handler;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Timer listener started — polling every {Seconds}s", Interval.TotalSeconds);

        using var timer = new PeriodicTimer(Interval);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var batch = _pending.TakeBatch(2);

            if (batch.Count == 0)
            {
                _logger.LogInformation("Nothing pending.");
                continue;
            }

            _logger.LogInformation(
                "Found {Count} pending ({Remaining} left in table)",
                batch.Count, _pending.Count);

            foreach (var order in batch)
            {
                try
                {
                    await _handler.HandleAsync(order, stoppingToken);
                }
                catch (Exception ex)
                {
                    // You own this. There is no dead-letter queue behind you.
                    // Put it back and it retries every tick, forever, and one bad
                    // order can spin here until someone reads the logs.
                    // Give up instead and the order is silently lost.
                    // Both options are bad, and both are yours to write.
                    _logger.LogError(ex,
                        "Order {OrderId} failed — putting it back", order.OrderId);
                    _pending.Return(order);
                }
            }
        }
    }
}
