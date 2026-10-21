using System.Threading.Channels;
using Confluent.Kafka;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Steelax.Pufflow.Sdk.Test.Kafka;
using Xunit;

namespace Steelax.Pufflow.Operators.Kafka.Tests;

/// <summary>
///     Unit tests for <see cref="KafkaConsumerProcessor{TKey,TValue}" /> driven by an in-memory
///     <see cref="TestConsumer{TKey,TValue}" /> (no Kafka container).
/// </summary>
public class KafkaConsumerProcessorTests
{
    private const int TimeoutMs = 10_000;

    private static KafkaConsumerOptions IdleAwareOptions() =>
        new(TimeSpan.FromMilliseconds(100))
        {
            EmergencyRatio = 0.1f,
            IdleRatio = 0.3f,
            WindowSize = 4,
            WindowLifetime = TimeSpan.FromMilliseconds(150),
            EmergencyCapacity = 16,
            AdvanceStrategy = AdvanceStrategy.ManualCommit,
            WatermarkAdvanceInterval = TimeSpan.FromMilliseconds(50)
        };

    /// <summary>
    ///     Drives the reader side of the pipeline: every emitted carrier (a record or a bare progress marker)
    ///     forwards its watermark to the shared tracker, mirroring what the real downstream reader does.
    /// </summary>
    private static async Task<Watermark[]> CollectAndCommitAsync(
        ChannelReader<Carrier<ConsumeResult<string, string>>> reader,
        WatermarkTracker tracker,
        CancellationToken cancellationToken)
    {
        var watermarks = new List<Watermark>();
        
        await foreach (var item in reader.ReadAllAsync(cancellationToken))
        {
            if (item.Watermark.IsNothing)
                continue;
            
            tracker.SetProgress(item.Watermark);
            watermarks.Add(item.Watermark);
        }
        
        return watermarks.ToArray();
    }

    /// <summary>
    ///     When polling returns no data, the processor emits bare progress markers periodically: one whenever the
    ///     fresh provider tick has advanced beyond the last emitted watermark by
    ///     <see cref="KafkaConsumerOptions.WatermarkAdvanceInterval" />. The markers keep the stream's watermark
    ///     alive while the source is quiet — the minimum of several merged sources — and let the delayed commit
    ///     point reach the last window so it is flushed (fixed).
    /// </summary>
    /// <remarks>
    ///     The test mirrors a quieting tail: one record, then an idle stream until end-of-stream. Several
    ///     periodical markers must be pushed (the provider keeps advancing with real time), each strictly newer
    ///     than the previous progress point, and the record must eventually be committed.
    /// </remarks>
    [Fact(Timeout = TimeoutMs)]
    public async Task IdleMarker_AdvancesPeriodically_CommitsLastWindow()
    {
        var consumer = new TestConsumer<string, string>(timeProvider: null);
        var tracker = WatermarkTracker.Create();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using var flow = new FlowSource();
        flow
            .OnKafkaSource(consumer, IdleAwareOptions(), tracker, new TestKafkaErrorPolicy())
            .Consume(out var reader);

        var readerTask = CollectAndCommitAsync(reader, tracker, cts.Token);
        var runTask = flow.ExecuteAsync(cts.Token);

        // One record, then an idle stream (bare progress markers) until end-of-stream.
        Assert.True(consumer.Writer.TryWrite(new KeyValuePair<string, string>("k0", "v0")));
        await Task.Delay(500, cts.Token);

        consumer.Writer.TryComplete();

        // The loop stops cleanly on end-of-stream (TestKafkaErrorPolicy aborts on TestChannelCompletedException).
        await runTask;
        var watermarks = await readerTask;

        // Every emitted progress point is delivered with a real watermark.
        Assert.NotEmpty(watermarks);
        Assert.All(watermarks, static w => Assert.False(w.IsNothing));

        // While the stream is quiet the source keeps resetting its progress: several distinct markers are
        // pushed (the merge case needs the watermark to keep advancing, not a single one-shot reset), and
        // each next marker advances beyond the previously emitted progress (the strict-newer guarantee).
        var markers = watermarks.Where(static w => w > Watermark.NothingValue).ToArray();
        Assert.True(markers.Length >= 3,
            $"periodic idle markers must be pushed while the source is quiet (seen: {markers.Length})");

        for (var i = 1; i < markers.Length; i++)
            Assert.True(markers[i - 1] < markers[i],
                "idle markers must be strictly increasing progress steps");

        // The record was committed: the periodic markers keep advancing the reader watermark past the open
        // window, so the delayed commit point eventually reaches it and the window is flushed.
        var committed = consumer.Commit();
        var partitionCommited = Assert.Single(committed);
        Assert.Equal("default", partitionCommited.Topic);
        Assert.Equal(0, partitionCommited.Partition.Value);
        Assert.Equal(0, partitionCommited.Offset.Value);
    }
}