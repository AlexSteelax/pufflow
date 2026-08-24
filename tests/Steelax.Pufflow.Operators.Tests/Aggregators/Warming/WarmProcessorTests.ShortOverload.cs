using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Unio;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class ShortOverload
    {
        [Fact(Timeout = 1_000)]
        public async Task CollapsesPassthroughAndGroupsIntoValues_WithFinalProgressWatermark()
        {
            var input = new List<Watermarked<int>>
            {
                new(1, Watermark.From(10)), // passthrough (odd)
                new(2, Watermark.From(20)), // warm
                new(3, Watermark.From(30)), // passthrough
                new(4, Watermark.From(40)) // warm
            };

            await using var flow = new FlowSource();
            var policy = new TestPolicy();

            var options = new WarmOptions
            {
                MaxConcurrency = 1,
                MaxQueued = 8,
                SegmentCapacity = 4,
                SegmentLinger = TimeSpan.FromMilliseconds(NoLingerMs),
                QueueWeightLimit = 1000,
                WatchdogPeriod = Timeout.InfiniteTimeSpan
            };

            flow
                .OnAsyncConsumatorSource(input)
                .Warming(
                    options,
                    new SyncJobFactory(),
                    ValueToKey,
                    policy,
                    new QueueAccumulatorFactory())
                .Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);

            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            // The short overload collapses both passthrough values (T0) and warmed groups (T1 of the
            // underlying 3-way union) into the single value slot — every input appears exactly once.
            // Emission order: passthrough values are written immediately when handled (1, 3), while the
            // warmed groups (2, 4) are held in the delayed queue and only drained when the segment is
            // sealed at end-of-stream (no linger, segment not full).
            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            Assert.Equal(new[] { 1, 3, 2, 4 }, values);

            // The progress watermark closes the stream as the final item.
            Assert.True(results[^1].IsT1, "watermark should be the last item");
            Assert.Equal(Watermark.From(40), results[^1].AsT1);

            Assert.Equal(2, policy.Warmed.Count);
        }
    }
}
