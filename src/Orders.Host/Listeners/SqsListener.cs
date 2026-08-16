using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Orders.Core;

namespace Orders.Host.Listeners;

/// <summary>
/// The "after" listener: the same job, a different delivery mechanism.
///
/// Structurally it matches TimerListener — same base class, same handler call,
/// same loop. Three things differ:
///
///   1. It waits on the queue instead of on a clock  → work starts in ms, not minutes.
///   2. Success means Delete                         → "processed" is now an ack.
///   3. Failure means do nothing                     → SQS redelivers, then dead-letters.
///
/// The third is the significant one: the error handling got *smaller*.
/// </summary>
public class SqsListener : BackgroundService
{
    // Producers in other languages will send camelCase. Being lenient here avoids
    // a baffling "every property is null" debugging session.
    private static readonly JsonSerializerOptions JsonOptions =
        new() { PropertyNameCaseInsensitive = true };

    private readonly IAmazonSQS _sqs;
    private readonly IOrderHandler _handler;
    private readonly IConfiguration _config;
    private readonly ILogger<SqsListener> _logger;

    public SqsListener(
        IAmazonSQS sqs,
        IOrderHandler handler,
        IConfiguration config,
        ILogger<SqsListener> logger)
    {
        _sqs = sqs;
        _handler = handler;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueUrl = await Sqs.EnsureQueueAsync(_sqs, _config, stoppingToken);
        _logger.LogInformation("SQS listener started — long-polling {QueueUrl}", queueUrl);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var response = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    MaxNumberOfMessages = 10,

                    // Long polling: the call parks server-side for up to 20s and
                    // returns the instant a message lands. This one property is why
                    // there is no PeriodicTimer in this file, and why latency is
                    // milliseconds instead of half a polling interval.
                    WaitTimeSeconds = 20,
                }, stoppingToken);

                // AWS SDK v4 returns null, not an empty list, when nothing arrived.
                if (response.Messages is null || response.Messages.Count == 0)
                    continue;

                foreach (var message in response.Messages)
                    await ProcessAsync(queueUrl, message, stoppingToken);
            }
            catch (Exception ex)
            {
                // Ctrl+C aborts the in-flight long poll. That is not an error —
                // and the SDK wraps it in TimeoutException, so check the token
                // rather than the exception type or you log noise on every shutdown.
                if (stoppingToken.IsCancellationRequested)
                    break;

                // Queue unreachable, creds expired, throttled. Back off and retry —
                // this guards the *connection*, not the business logic.
                _logger.LogError(ex, "Receive failed — backing off 5s");

                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task ProcessAsync(string queueUrl, Message message, CancellationToken ct)
    {
        OrderPlaced? order;
        try
        {
            order = JsonSerializer.Deserialize<OrderPlaced>(message.Body, JsonOptions);
        }
        catch (JsonException ex)
        {
            // Unparseable will never parse. Retrying is pointless, so drop it and
            // let the redrive policy move it to the DLQ for a human to look at.
            _logger.LogError(ex, "Malformed message {MessageId}", message.MessageId);
            return;
        }

        if (order is null) return;

        try
        {
            await _handler.HandleAsync(order, ct);

            // The ack. Until this line runs, SQS still considers the message in flight
            // and will hand it to someone else once the visibility timeout expires.
            await _sqs.DeleteMessageAsync(queueUrl, message.ReceiptHandle, ct);
        }
        catch (Exception ex)
        {
            // Deliberately no retry loop and no re-queue. Not deleting *is* the nack:
            // the message reappears after the visibility timeout, and after
            // maxReceiveCount attempts SQS moves it to the DLQ on its own. Compare
            // the equivalent catch block in TimerListener, which has to implement
            // all of that by hand and still ends up looping on a poison record.
            _logger.LogError(ex,
                "Order {OrderId} failed — leaving it for redelivery", order.OrderId);
        }
    }
}
