namespace Orders.Core;

/// <summary>
/// The message. Plain data — no AWS types, no attributes, no base class.
/// The same shape travels whether it came off a queue or out of a database.
/// </summary>
public record OrderPlaced(
    string OrderId,
    string Customer,
    decimal Amount);
