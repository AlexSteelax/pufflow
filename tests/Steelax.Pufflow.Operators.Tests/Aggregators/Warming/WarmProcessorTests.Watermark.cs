using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class WatermarkSequence
    {
        [Fact(Timeout = 1_000)]
        public async Task MixedMode_StrictlyIncreasingWatermarks()
        {
            // Non-uniform mixed mode with accumulation: values cycle 1..8, 0 (mod 9), key equals the
            // value, so keys are reused and accumulate into an honest per-key queue (TValue == TGroup,
            // each value is released exactly one at a time). We warm values with remainder 5..8 (about
            // half of the stream), the rest pass through. Input watermarks are non-decreasing and repeat
            // three times each (as a real provider would within one clock tick), so the output watermarks
            // (segment-covering + final global) must be non-decreasing вЂ” head-of-line segment emission.
            const int n = 100;
            const int modulo = 9;
            var input = Enumerable.Range(1, n)
                .Select(i => new Carrier<int>(i % modulo, Watermark.From((i + 1) * 10 / 3)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(static key => key >= 5);

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                policy,
                new QueueAccumulatorFactory(),
                input,
                flow,
                DefaultOptions(),
                TestContext.Current.CancellationToken);

            // Mixed mode: both passthrough values and warmed groups are present.
            var values = Values(results);
            var groups = Groups(results);
            Assert.NotEmpty(values);
            Assert.NotEmpty(groups);

            // Every value is released exactly once: passthrough values plus warmed groups
            // must total the number of input values вЂ” nothing is lost and nothing is duplicated.
            Assert.Equal(n, values.Length + groups.Length);

            // All real (non-Nothing) progress watermarks are non-decreasing: they may repeat, as the input
            // watermarks repeat within a tick, but collapsing consecutive duplicates yields a strictly
            // increasing sequence вЂ” progress never goes backwards.
            var watermarks = Progress(results);
            var real = watermarks.Where(static w => !w.IsNothing).ToArray();
            Assert.NotEmpty(real);
            Assert.OrderIncreasing(real, false);

            // The maximum real watermark equals the maximum of the input and closes the pipeline.
            var maxInput = Watermark.From((n + 1) * 10 / 3);
            Assert.Equal(maxInput, real.Max());
        }

        [Fact(Timeout = 1_000)]
        public async Task MonotonicWatermarks_OneKey_LargeInput_LastWatermarkEmitted()
        {
            // The same warmable key on 500 positions, the watermark grows with each message but repeats
            // three times (as a real provider would within one clock tick). The output watermark is
            // exactly the last (maximum) one.
            const int n = 30;
            var input = Enumerable.Range(0, n)
                .Select(i => new Carrier<int>(2, Watermark.From((i + 1) * 10 / 3)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(WarmEvenOnly); // warm even keys (2)

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                DefaultOptions() with { SegmentTtl = TimeSpan.FromMilliseconds(50) },
                TestContext.Current.CancellationToken);

            // The key is warmable вЂ” there must be no passthrough.
            Assert.DoesNotContain(results, static r => r is { HasValue: true, Value.IsT0: true });

            // All values of the key are accumulated into a single group (one group per key).
            Assert.Equal(1, results.Count(static r => r is { HasValue: true, Value.IsT1: true }));

            // Real (non-Nothing) progress watermarks are non-decreasing (consecutive duplicates collapsed)
            // and never exceed the input maximum.
            var watermarks = Progress(results);
            var real = watermarks.Where(static w => !w.IsNothing).ToArray();
            Assert.NotEmpty(real);
            Assert.OrderIncreasing(real, false);
            Assert.All(real, w => Assert.True(w <= Watermark.From(n * 10 / 3)));

            // The final (global progress) watermark is exactly the last of the input.
            Assert.True(IsLastProgress(results), "watermark should be the last item");
            Assert.Equal(Watermark.From(n  * 10 / 3), results[^1].Watermark);
        }

        [Fact(Timeout = 1_000)]
        public async Task MonotonicWatermarks_UniqueKeys_LargeInput_LastWatermarkEmitted()
        {
            // Honest per-key queue (TValue == TGroup, short Warming): 500 unique warmable keys, watermark grows
            // but repeats three times (as a real provider would within one clock tick). Each key releases exactly
            // its own value once; progress watermarks stay non-decreasing and close on the last input maximum.
            const int n = 500;
            var input = Enumerable.Range(0, n)
                .Select(i => new Carrier<int>(i * 2, Watermark.From(((i + 1) / 3) * 10)))
                .ToArray();

            var carriers = input.Select(static w => new Carrier<int>(w.Value, w.Watermark)).ToArray();

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(WarmEvenOnly); // warm even keys

            var options = new WarmOptions
            {
                MaxConcurrency = 1,
                MaxQueued = 8,
                SegmentCapacity = 4,
                SegmentTtl = TimeSpan.FromMilliseconds(NoLingerMs),
                QueueWeightLimit = 1000,
                WatchdogPeriod = Timeout.InfiniteTimeSpan
            };

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

            // Every key is a distinct group and is released exactly once (in entry order) as its own value.
            var values = results.Where(static r => r.HasValue).Select(static r => r.Value).ToArray();
            Assert.Equal(n, values.Length);
            Assert.Equal(Enumerable.Range(0, n).Select(i => i * 2), values);

            // Real (non-Nothing) progress watermarks are non-decreasing; consecutive duplicates collapse.
            var real = results.Where(static r => !r.HasValue).Select(static r => r.Watermark)
                .Where(static w => !w.IsNothing).ToArray();
            Assert.NotEmpty(real);
            Assert.OrderIncreasing(real, false);
            Assert.All(real, w => Assert.True(w <= Watermark.From((n / 3) * 10)));

            // The maximum real watermark equals the input maximum and closes the pipeline.
            var maxInput = Watermark.From((n / 3) * 10);
            Assert.True(!results[^1].HasValue, "watermark should be the last item");
            Assert.Equal(maxInput, results[^1].Watermark);
        }
    }
}

