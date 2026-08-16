using Orders.Core;

namespace Orders.Host.Listeners;

/// <summary>
/// Stands in for the table the timer listener polls —
/// the classic "SELECT * FROM Orders WHERE Processed = 0".
///
/// Lives in the host, not in Orders.Core, because polling a table is a delivery
/// detail. Nothing in Orders.Core knows this class exists.
/// </summary>
public class PendingOrders
{
    private readonly List<OrderPlaced> _pending =
    [
        new("ORD-001", "Alice", 42.00m),
        new("ORD-002", "Bob", 17.50m),
        new("ORD-003", "Carol", 99.99m),
        new("ORD-004", "Dave", -5.00m),   // rejected by the handler, on purpose
        new("ORD-005", "Erin", 12.00m),
    ];

    private readonly Lock _gate = new();

    public IReadOnlyList<OrderPlaced> TakeBatch(int max)
    {
        lock (_gate)
        {
            var batch = _pending.Take(max).ToList();
            _pending.RemoveRange(0, batch.Count);
            return batch;
        }
    }

    /// <summary>
    /// A failed order goes back in the table, because nothing marked it processed —
    /// so it comes back on the next tick, and the next, indefinitely.
    /// </summary>
    public void Return(OrderPlaced order)
    {
        lock (_gate)
        {
            _pending.Add(order);
        }
    }

    public int Count
    {
        get { lock (_gate) { return _pending.Count; } }
    }
}
