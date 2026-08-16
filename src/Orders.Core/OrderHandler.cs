using Microsoft.Extensions.Logging;

namespace Orders.Core;

/// <summary>
/// The core module. This is the file to point at and say:
/// "this does not change when we move to SQS."
///
/// It has no idea a queue exists. It cannot tell you whether it was invoked
/// by a timer or by a long-poll. That ignorance is the design.
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
        // Deliberately boring — the business logic is not the subject of the course.
        await Task.Delay(TimeSpan.FromMilliseconds(200), ct);

        // Throwing here is how the handler says "I failed."
        // How that failure is *recovered* is the listener's problem, and the two
        // listeners answer it very differently. That contrast is the lesson.
        if (order.Amount < 0)
            throw new InvalidOperationException(
                $"Order {order.OrderId} has a negative amount.");

        _logger.LogInformation("Done with order {OrderId}", order.OrderId);
    }
}
