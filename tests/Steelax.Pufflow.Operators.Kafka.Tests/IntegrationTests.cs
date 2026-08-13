using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Steelax.Pufflow;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Kafka;
using Steelax.Pufflow.Operators.Kafka.Tests.Fixtures;
using Steelax.Pufflow.Sdk.Test;
using Xunit;

namespace Steelax.Pufflow.Operators.Kafka.Tests;

/// <summary>
///     Integration tests for <see cref="KafkaConsumerProcessor{TKey,TValue}" /> using a real Kafka
///     container. The processor is treated as a push source: the minimal pipeline is
///     <c>FlowSource → KafkaConsumerProcessor → FlowSinkProducator</c> — a source and a consumer, with no
///     intermediate stages.
/// </summary>
/// <remarks>
///     A Kafka source has no natural end-of-stream, so the tests never await
///     <see cref="FlowSource.ExecuteAsync" /> (it would run forever): the pipeline is started in the
///     background, the reader drains exactly the expected number of items via
///     <see cref="ChannelReader{T}.WaitToReadAsync" /> / <see cref="ChannelReader{T}.TryRead" />, and then
///     the flow is cancelled to stop the consume loop.
/// </remarks>
public class IntegrationTests(ApplicationFixture application, ITestOutputHelper output)
{
    private static readonly KafkaConsumerOptions DefaultOptions = new(TimeSpan.FromMilliseconds(100))
    {
        EmergencyRatio = 0.1f,
        IdleRatio = 0.1f,
        WindowSize = 4,
        WindowLifetime = TimeSpan.FromMilliseconds(250),
        EmergencyCapacity = 16,
        AdvanceStrategy = AdvanceStrategy.ManualCommit
    };

    private string CreateTopicName([System.Runtime.CompilerServices.CallerMemberName] string test = "") =>
        $"pufflow-{test.ToLowerInvariant()}-{Guid.NewGuid():N}";

    /// <summary>Creates the topic if it does not exist, so the consumer never hits "Unknown topic or partition".</summary>
    private static async Task EnsureTopicAsync(string bootstrap, string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
    }

    private static async Task ProduceAsync(string bootstrap, string topic, int count)
    {
        using var producer = new ProducerBuilder<string, string>(
                new ProducerConfig { BootstrapServers = bootstrap })
            .Build();

        for (var i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = i.ToString(),
                Value = $"value-{i}"
            });
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    private IConsumer<string, string> CreateConsumer(string bootstrap, string groupId) =>
        new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();

    /// <summary>Reads exactly <paramref name="count" /> items from the reader, waiting with the given token.</summary>
    private static async Task<List<Watermarked<ConsumeResult<string, string>>>> ReadExactlyAsync(
        ChannelReader<Watermarked<ConsumeResult<string, string>>> reader,
        int count,
        CancellationToken cancellationToken)
    {
        var items = new List<Watermarked<ConsumeResult<string, string>>>(count);

        while (items.Count < count)
        {
            if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                break;

            while (items.Count < count && reader.TryRead(out var item))
                items.Add(item);
        }

        return items;
    }

    [Fact(Timeout = 30_000)]
    public async Task Kafka_ProducedMessages_AreConsumedInOrder()
    {
        var bootstrap = application.KafkaContainer.GetBootstrapAddress();
        var topic = CreateTopicName();
        const int count = 20;

        await ProduceAsync(bootstrap, topic, count);

        using var consumer = CreateConsumer(bootstrap, $"grp-{Guid.NewGuid():N}");
        consumer.Subscribe(topic);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using var flow = new FlowSource(cts.Token);
        flow
            .OnKafkaSource(consumer, DefaultOptions, KafkaErrorPolicy.Default)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(); // background: the Kafka source has no end-of-stream

        try
        {
            var items = await ReadExactlyAsync(reader, count, cts.Token);

            // All messages are delivered, in the produced order.
            Assert.Equal(count, items.Count);
            Assert.Equal(Enumerable.Range(0, count).Select(i => $"value-{i}"), items.Select(i => i.Value.Message.Value));

            // The system watermark provider emits Watermark.Nothing() for records consumed within the same
            // tick (Environment.TickCount64 changes roughly every 15.6 ms). Filtering those out, the
            // remaining (real) watermarks must be strictly increasing — progress never goes backwards.
            var progress = items.Where(i => !i.IsNothing).Select(i => i.Watermark).ToList();
            Assert.NotEmpty(progress);

            for (var i = 1; i < progress.Count; i++)
                Assert.True(progress[i - 1] < progress[i],
                    "real (non-Nothing) watermarks must be strictly increasing");
        }
        finally
        {
            // Stop the Kafka consume loop; do not await ExecuteAsync — it never completes on its own.
            await cts.CancelAsync();
        }
    }

    [Fact(Timeout = 30_000)]
    public async Task Kafka_EmptyTopic_ConsumesNothing_UntilCancellation()
    {
        var bootstrap = application.KafkaContainer.GetBootstrapAddress();
        var topic = CreateTopicName();
        await EnsureTopicAsync(bootstrap, topic);

        using var consumer = CreateConsumer(bootstrap, $"grp-{Guid.NewGuid():N}");
        consumer.Subscribe(topic);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using var flow = new FlowSource(cts.Token);
        flow
            .OnKafkaSource(consumer, DefaultOptions, KafkaErrorPolicy.Default)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(); // background: the Kafka source has no end-of-stream

        try
        {
            // Wait briefly: nothing is produced, so nothing arrives on the reader.
            await Task.Delay(500, cts.Token);

            // No items were emitted on an empty topic.
            Assert.False(reader.TryRead(out _), "an empty topic must not emit any items");
        }
        finally
        {
            await cts.CancelAsync();
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task Kafka_Idle_WithSmallClientTimeouts_KeepsConnectionAlive()
    {
        // Scenario: the consumer is configured with aggressive client timeouts (small Session.Timeout and
        // MaxPollInterval). On an empty topic the loop falls back to the Idle mode (polling at IdleInterval).
        // The pipeline must stay alive longer than the client timeouts — the idle polling must keep the
        // connection healthy — and afterwards must still consume newly produced messages.
        var bootstrap = application.KafkaContainer.GetBootstrapAddress();
        var topic = CreateTopicName();
        await EnsureTopicAsync(bootstrap, topic);

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"grp-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            // The broker enforces group.min.session.timeout.ms = 6000 by default; 5000 is rejected with
            // "Invalid session timeout". Use the minimum allowed value to keep the test aggressive.
            SessionTimeoutMs = 6000,
            MaxPollIntervalMs = 6000
        }).Build();

        consumer.Subscribe(topic);

        // Idle polling interval well below the client timeouts, so Consume() keeps the group session alive.
        var options = new KafkaConsumerOptions(TimeSpan.FromMilliseconds(100))
        {
            EmergencyRatio = 0.1f,
            IdleRatio = 0.1f,
            WindowSize = 4,
            WindowLifetime = TimeSpan.FromMilliseconds(250),
            EmergencyCapacity = 16,
            AdvanceStrategy = AdvanceStrategy.ManualCommit
        };

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using var flow = new FlowSource(cts.Token);
        flow
            .OnKafkaSource(consumer, options, KafkaErrorPolicy.Default)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(); // background: the Kafka source has no end-of-stream

        try
        {
            // Stay idle well beyond the client's Session.Timeout (5s). The idle loop must keep the
            // connection alive: the pipeline must not fault.
            await Task.Delay(TimeSpan.FromSeconds(12), cts.Token);

            Assert.False(runTask.IsFaulted, "the idle Kafka source must not fault within the client timeouts");
            Assert.False(reader.TryRead(out _), "no messages should be produced on an empty topic");

            // The connection is still alive: a message produced now must be delivered.
            await ProduceAsync(bootstrap, topic, 1);

            var item = await ReadExactlyAsync(reader, 1, cts.Token);
            var message = Assert.Single(item);
            Assert.Equal("value-0", message.Value.Message.Value);
        }
        finally
        {
            // Surface the pipeline fault (if any) in the test output for diagnostics.
            if (runTask.IsFaulted)
                output.WriteLine($"Pipeline faulted: {runTask.Exception}");
            
            await cts.CancelAsync();
        }
    }
}
