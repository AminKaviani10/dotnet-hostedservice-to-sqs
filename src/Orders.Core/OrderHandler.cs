using Microsoft.Extensions.Logging;

namespace Orders.Core;

/// <summary>
/// The core module, shared by every listener and unchanged by the move to SQS.
///
/// It has no idea a queue exists, and no way to tell whether it was invoked by a
/// timer or by a long poll. That ignorance is the design: delivery can be replaced
/// without touching this file.
/// </summary>
public class OrderHandler : IOrderHandler
{
    private readonly ILogger<OrderHandler> _logger;

    public OrderHandler(ILogger<OrderHandler> logger) => _logger = logger;

    public async Task HandleAsync(OrderPlaced order, CancellationToken ct)
    {
        _logger.LogInformation(
            "Processing order {OrderId} for {Customer} (${Amount})",
            order.OrderId, order.Customer, order.Amount);

        // Stand-in for the real work: charge a card, write a row, call an API.
        // Kept trivial on purpose; the business logic is not the interesting part.
        await Task.Delay(TimeSpan.FromMilliseconds(200), ct);

        // Throwing is how the handler reports failure. How that failure is
        // *recovered* is the listener's problem, and the two listeners answer it
        // very differently — compare their catch blocks.
        if (order.Amount < 0)
            throw new InvalidOperationException(
                $"Order {order.OrderId} has a negative amount.");

        _logger.LogInformation("Done with order {OrderId}", order.OrderId);
    }
}
