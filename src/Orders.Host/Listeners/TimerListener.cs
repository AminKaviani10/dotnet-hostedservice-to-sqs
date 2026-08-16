using Orders.Core;

namespace Orders.Host.Listeners;

/// <summary>
/// The "before" listener: the conventional polling BackgroundService.
///
/// It wakes on an interval, goes looking for work, and calls the handler. Every
/// part of it is application code, including every failure decision.
/// </summary>
public class TimerListener : BackgroundService
{
    // Production intervals are usually minutes; kept short here.
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
                    // This is the weak point of the whole approach, and there is no
                    // dead-letter queue behind it. Putting the order back means it
                    // retries every tick forever, so one bad record spins here until
                    // somebody reads the logs. Dropping it instead loses the order
                    // silently. Both options are bad, and both have to be written by
                    // hand — along with a retry counter, a failures table, and row
                    // locking once more than one instance runs.
                    //
                    // SqsListener replaces all of that with a redrive policy.
                    _logger.LogError(ex,
                        "Order {OrderId} failed — putting it back", order.OrderId);
                    _pending.Return(order);
                }
            }
        }
    }
}
