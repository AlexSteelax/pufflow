using System.Threading.Channels;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Steelax.Pufflow.Operators.Common;
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

    /// <summary>
    ///     Collects raw items from the reader until exactly <paramref name="count" /> carried <b>records</b>
    ///     (<see cref="Carrier{T}.HasValue" /> elements) have been seen. Empty (bare-progress) carriers that may
    ///     appear while idle are collected as well, so watermark assertions still see the full emitted sequence.
    /// </summary>
    private static async Task<List<Carrier<ConsumeResult<string, string>>>> CollectAsync(
        ChannelReader<Carrier<ConsumeResult<string, string>>> reader,
        int count,
        CancellationToken cancellationToken)
    {
        var items = new List<Carrier<ConsumeResult<string, string>>>();
        var records = 0;

        while (records < count)
        {
            if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                break;

            while (records < count && reader.TryRead(out var item))
            {
                items.Add(item);
                if (item.HasValue)
                    records++;
            }
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
        await using var flow = new FlowSource();
        flow
            .OnKafkaSource(consumer, DefaultOptions, out _, KafkaErrorPolicy.Default)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(cts.Token); // background: the Kafka source has no end-of-stream

        try
        {
            // Collect raw items (records + bare progress) until we have seen all records.
            var items = await CollectAsync(reader, count, cts.Token);
            var records = items.Where(static it => it.HasValue).Select(static it => it.Value).ToList();

            // All messages are delivered, in the produced order.
            Assert.Equal(count, records.Count);
            Assert.Equal(Enumerable.Range(0, count).Select(i => $"value-{i}"), records.Select(i => i.Message.Value));

            // Records carry a real watermark (never Nothing) and progress across records never goes
            // backwards (values may repeat within a single running tick — no strict advancement required).
            var progressWatermarks = items.Where(static it => it.HasValue).Select(static it => it.Watermark).ToList();
            Assert.DoesNotContain(items.Where(static it => it.HasValue), static it => it.Watermark.IsNothing);

            for (var i = 1; i < progressWatermarks.Count; i++)
                Assert.True(progressWatermarks[i - 1] <= progressWatermarks[i],
                    "watermarks must be non-decreasing — progress never goes backwards");
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
        await using var flow = new FlowSource();
        flow
            .OnKafkaSource(consumer, DefaultOptions, out _, KafkaErrorPolicy.Default)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(cts.Token); // background: the Kafka source has no end-of-stream

        try
        {
            // Wait several waiting cycles: the idle Kafka source should have emitted bare progress markers,
            // but never a consumed record on an empty topic.
            await Task.Delay(500, cts.Token);

            var sawRecord = false;
            var sawMarker = false;
            while (reader.TryRead(out var item))
            {
                if (item.HasValue)
                    sawRecord = true;
                else
                    sawMarker = true;
            }

            Assert.False(sawRecord, "an empty topic must not emit any consumed records");
            Assert.True(sawMarker, "idle consumption should emit bare progress markers");
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
        await using var flow = new FlowSource();
        flow
            .OnKafkaSource(consumer, options, out _, KafkaErrorPolicy.Default)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(cts.Token); // background: the Kafka source has no end-of-stream

        try
        {
            // Stay idle well beyond the client's Session.Timeout. While idle the source should emit bare
            // progress markers (which keeps the pipeline alive) but never consumed records on an empty topic.
            await Task.Delay(TimeSpan.FromSeconds(12), cts.Token);

            Assert.False(runTask.IsFaulted, "the idle Kafka source must not fault within the client timeouts");

            bool sawRecordOnIdle = false, sawMarkerOnIdle = false;
            while (reader.TryRead(out var idleItem))
            {
                if (idleItem.HasValue)
                    sawRecordOnIdle = true;
                else
                    sawMarkerOnIdle = true;
            }

            Assert.False(sawRecordOnIdle, "no consumed records should be produced while idle on an empty topic");
            Assert.True(sawMarkerOnIdle, "idle consumption should emit bare progress markers");

            // The connection is still alive: a message produced now must be delivered.
            await ProduceAsync(bootstrap, topic, 1);

            var collected = await CollectAsync(reader, 1, cts.Token);
            var value = Assert.Single(collected, static it => it.HasValue);
            Assert.Equal("value-0", value.Value.Message.Value);
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
