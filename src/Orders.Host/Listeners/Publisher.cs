using System.Text.Json;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Orders.Core;

namespace Orders.Host.Listeners;

/// <summary>
/// One-shot commands for exercising the queue from the command line:
///   dotnet run -- send Alice 42     put an order on the queue
///   dotnet run -- send Dave -5      an order the handler rejects
///   dotnet run -- dlq               list whatever was dead-lettered
///   dotnet run -- reset             empty both queues
/// </summary>
public static class Publisher
{
    private static IConfiguration BuildConfig() =>
        new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

    public static async Task SendAsync(string[] args)
    {
        var config = BuildConfig();

        var customer = args.Length > 1 ? args[1] : "Alice";
        var amount = args.Length > 2 ? decimal.Parse(args[2]) : 42m;

        var order = new OrderPlaced(
            OrderId: $"ORD-{Guid.NewGuid().ToString()[..8]}",
            Customer: customer,
            Amount: amount);

        var sqs = Sqs.CreateClient(config);
        var queueUrl = await Sqs.EnsureQueueAsync(sqs, config, default);

        await sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = JsonSerializer.Serialize(order),
        });

        Console.WriteLine($"Sent {order.OrderId} ({customer}, ${amount})");
    }

    /// <summary>
    /// Lists the dead-letter queue without consuming from it, so it can be run
    /// repeatedly. A message shown here is off the main queue for good: SQS gave up
    /// on it and parked it somewhere a human can inspect it.
    /// </summary>
    public static async Task PeekDeadLetterAsync()
    {
        var config = BuildConfig();
        var sqs = Sqs.CreateClient(config);

        // Setup is idempotent, so this also works before the listener has ever run.
        await Sqs.EnsureQueueAsync(sqs, config, default);
        var dlqUrl = (await sqs.CreateQueueAsync(Sqs.DlqName(config))).QueueUrl;

        var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = dlqUrl,
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 2,
            VisibilityTimeout = 0,   // put it straight back, so this is read-only
        });

        var messages = response.Messages ?? [];

        Console.WriteLine($"=== Dead-letter queue: {messages.Count} message(s) ===");
        foreach (var m in messages)
            Console.WriteLine($"  {m.Body}");

        if (messages.Count == 0)
            Console.WriteLine("  (nothing yet — send a failing order and let it retry)");
    }

    /// <summary>
    /// Empties both queues so the sample can be run again from a clean state.
    /// AWS throttles PurgeQueue to once per 60s per queue; LocalStack is lenient.
    /// </summary>
    public static async Task ResetAsync()
    {
        var config = BuildConfig();
        var sqs = Sqs.CreateClient(config);

        var mainUrl = await Sqs.EnsureQueueAsync(sqs, config, default);
        var dlqUrl = (await sqs.CreateQueueAsync(Sqs.DlqName(config))).QueueUrl;

        foreach (var (label, url) in new[] { ("orders", mainUrl), ("dlq", dlqUrl) })
        {
            try
            {
                await sqs.PurgeQueueAsync(url);
                Console.WriteLine($"Purged {label}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Could not purge {label}: {ex.Message}");
            }
        }
    }
}
