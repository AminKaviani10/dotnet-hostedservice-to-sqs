namespace Orders.Core;

/// <summary>
/// The seam.
///
/// Every listener depends on this and nothing else. Note what is absent:
/// no message id, no receipt handle, no "delete when done", no retry count.
/// Those are delivery concerns, and they stay in the listener.
///
/// The handler gets an order and does the work. That is the whole contract.
/// </summary>
public interface IOrderHandler
{
    Task HandleAsync(OrderPlaced order, CancellationToken ct);
}
