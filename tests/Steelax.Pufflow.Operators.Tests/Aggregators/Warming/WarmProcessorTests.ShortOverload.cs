using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class ShortOverload
    {
        [Fact(Timeout = 1_000)]
        public async Task CollapsesPassthroughAndGroupsIntoValues_WithFinalProgressWatermark()
        {
            var input = new List<Carrier<int>>
            {
                new(1, Watermark.From(10)), // passthrough (odd)
                new(2, Watermark.From(20)), // warm
                new(3, Watermark.From(30)), // passthrough
                new(4, Watermark.From(40)) // warm
            };

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(WarmEvenOnly);

            var options = new WarmOptions
            {
                MaxConcurrency = 1,
                MaxQueued = 8,
                SegmentCapacity = 2,
                SegmentTtl = TimeSpan.FromMilliseconds(NoLingerMs),
                QueueWeightLimit = 1000,
                WatchdogPeriod = Timeout.InfiniteTimeSpan
            };

            // The short overload consumes Carrier<int> and collapses warmed groups into plain values.
            var carriers = input.Select(static w => new Carrier<int>(w.Value, w.Watermark)).ToArray();

            flow
                .OnAsyncConsumatorSource(carriers)
                .Warming(
                    options,
                    new WarmingHelper.SyncJobFactory(),
                    ValueToKey,
                    policy,
                    new QueueAccumulatorFactory())
                .Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);

            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            // SegmentCapacity = 2 so the two warm keys (2, 4) fill the segment by size — no linger needed.
            // The short overload collapses both passthrough values and warmed results into the single value slot;
            // every input appears exactly once. Emission order: passthrough values are written immediately when
            // handled (1, 3), while the warmed values (2, 4) are held in the delayed queue and drained when the
            // segment completes. The segment's covering watermark (40) is emitted last as an empty (bare-progress)
            // carrier, so only data carriers contain the emitted values.
            var values = results.Where(static r => r.HasValue).Select(static r => r.Value).ToArray();
            Assert.Equal(new[] { 1, 3, 2, 4 }, values);

            Assert.Equal(4, results.Count(static r => r.HasValue));
            Assert.Single(results, static r => !r.HasValue); // exactly one bare-progress item

            // All progress watermarks are non-decreasing and the maximum closes the stream.
            var progress = results.Where(static r => r.HasWatermark).Select(static r => r.Watermark).ToArray();
            Assert.NotEmpty(progress);
            Assert.OrderIncreasing(progress, false);
            Assert.Equal(Watermark.From(40), progress.Max());

            // The progress watermark closes the stream as the final empty (bare-progress) carrier item.
            Assert.True(!results[^1].HasValue, "watermark should be the last item");
            Assert.Equal(Watermark.From(40), results[^1].Watermark);

            Assert.Equal(2, policy.PlainItems.Count);
        }
    }
}

