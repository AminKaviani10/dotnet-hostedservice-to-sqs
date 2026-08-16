using System.Text.Json;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace Orders.Host.Listeners;

/// <summary>
/// Client wiring and queue setup. Mostly boring — but read EnsureQueueAsync,
/// because the redrive policy is the thing that makes the SQS story true.
/// </summary>
public static class Sqs
{
    /// <summary>Give up on a message after this many failed attempts.</summary>
    private const int MaxReceiveCount = 3;

    /// <summary>
    /// How long a failed message stays hidden before SQS offers it again.
    /// AWS defaults to 30s; 10s keeps the retries on camera.
    /// In production this must exceed your handler's worst-case runtime.
    /// </summary>
    private const int VisibilityTimeoutSeconds = 10;

    public static IAmazonSQS CreateClient(IConfiguration config)
    {
        var serviceUrl = config["Sqs:ServiceUrl"];

        // No ServiceUrl configured → real AWS, normal credential chain
        // (env vars, ~/.aws/credentials, IAM role).
        if (string.IsNullOrWhiteSpace(serviceUrl))
            return new AmazonSQSClient();

        // ServiceUrl configured → LocalStack. Credentials are ignored by
        // LocalStack but the SDK still insists on finding some.
        return new AmazonSQSClient(
            new BasicAWSCredentials("test", "test"),
            new AmazonSQSConfig
            {
                ServiceURL = serviceUrl,
                AuthenticationRegion = "us-east-1",
                MaxErrorRetry = 1,

                // MUST stay above SqsListener's WaitTimeSeconds (20s), or this
                // timeout kills every long poll and the log fills with
                // TimeoutExceptions. Fail-fast at startup is handled below,
                // with its own short deadline.
                Timeout = TimeSpan.FromSeconds(30),
            });
    }

    public static string QueueName(IConfiguration config)
        => config["Sqs:QueueName"] ?? "orders";

    public static string DlqName(IConfiguration config)
        => QueueName(config) + "-dlq";

    /// <summary>
    /// Creates the dead-letter queue, the main queue, and the redrive policy that
    /// links them, then returns the main queue URL.
    ///
    /// The redrive policy is the whole argument for this course: after
    /// <see cref="MaxReceiveCount"/> failed attempts, SQS moves the message to the
    /// DLQ by itself. You do not write that code. There is no equivalent in
    /// TimerListener — there, a poison message loops until someone notices.
    ///
    /// In production this lives in Terraform/CDK, not in the app. It's here so the
    /// demo needs no AWS CLI step.
    /// </summary>
    public static async Task<string> EnsureQueueAsync(
        IAmazonSQS sqs, IConfiguration config, CancellationToken ct)
    {
        // Short deadline so a missing LocalStack shows up in seconds instead of
        // hanging the demo. Scoped to setup, not to the long-polling client.
        using var startup = CancellationTokenSource.CreateLinkedTokenSource(ct);
        startup.CancelAfter(TimeSpan.FromSeconds(10));

        try
        {
            // 1. The dead-letter queue, and its ARN.
            var dlq = await sqs.CreateQueueAsync(DlqName(config), startup.Token);
            var dlqAttrs = await sqs.GetQueueAttributesAsync(
                dlq.QueueUrl, ["QueueArn"], startup.Token);

            // 2. The main queue. Plain create keeps this idempotent — passing
            //    attributes here fails if the queue already exists with others.
            var main = await sqs.CreateQueueAsync(QueueName(config), startup.Token);

            // 3. Wire them together.
            await sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest
            {
                QueueUrl = main.QueueUrl,
                Attributes = new Dictionary<string, string>
                {
                    ["VisibilityTimeout"] = VisibilityTimeoutSeconds.ToString(),
                    ["RedrivePolicy"] = JsonSerializer.Serialize(new
                    {
                        deadLetterTargetArn = dlqAttrs.QueueARN,
                        maxReceiveCount = MaxReceiveCount.ToString(),
                    }),
                },
            }, startup.Token);

            return main.QueueUrl;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            throw new InvalidOperationException(
                $"Could not reach SQS at {config["Sqs:ServiceUrl"] ?? "AWS"}. " +
                "Is LocalStack running? Try: docker compose up -d", ex);
        }
    }
}
