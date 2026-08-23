using Steelax.Pufflow.Operators.Common;

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
            // (segment-covering + final global) must be non-decreasing — head-of-line segment emission.
            const int n = 100;
            const int modulo = 9;
            var input = Enumerable.Range(1, n)
                .Select(i => new Watermarked<int>(i % modulo, Watermark.From(((i + 1) / 3) * 10)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new PredicatePolicy(static key => key >= 5);

            var results = await RunAsync(
                new SyncJobFactory(),
                policy,
                new QueueAccumulatorFactory(),
                input,
                flow,
                null,
                TestContext.Current.CancellationToken);

            // Mixed mode: both passthrough values and warmed groups are present.
            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();
            Assert.NotEmpty(values);
            Assert.NotEmpty(groups);

            // Every value is released exactly once: passthrough values (T0) plus warmed groups (T1)
            // must total the number of input values — nothing is lost and nothing is duplicated.
            Assert.Equal(n, values.Length + groups.Length);

            // All real (non-Nothing) output watermarks are non-decreasing: they may repeat, as the input
            // watermarks repeat within a tick, but collapsing consecutive duplicates yields a strictly
            // increasing sequence — progress never goes backwards.
            var watermarks = results.Where(static r => r.IsT2).Select(static r => r.AsT2).ToArray();
            var real = watermarks.Where(static w => !w.IsNothing).ToArray();
            Assert.NotEmpty(real);
            Assert.DoesNotContain(watermarks, static w => w.IsNothing);
            AssertX.StrictlyIncreasing(real);

            // The maximum real watermark equals the maximum of the input and closes the pipeline.
            var maxInput = Watermark.From(((n + 1) / 3) * 10);
            Assert.Equal(maxInput, real.Max());
        }

        [Fact(Timeout = 1_000)]
        public async Task MonotonicWatermarks_OneKey_LargeInput_LastWatermarkEmitted()
        {
            // The same warmable key on 500 positions, the watermark grows with each message but repeats
            // three times (as a real provider would within one clock tick). The output watermark is
            // exactly the last (maximum) one.
            const int n = 500;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(2, Watermark.From(((i + 1) / 3) * 10)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // warm even keys (2)

            var results = await RunAsync(
                new SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                null,
                TestContext.Current.CancellationToken);

            // The key is warmable — there must be no passthrough.
            Assert.DoesNotContain(results, static r => r.IsT0);

            // All values of the key are accumulated into a single group (one group per key).
            Assert.Equal(1, results.Count(static r => r.IsT1));

            // Real (non-Nothing) output watermarks are non-decreasing (consecutive duplicates collapsed)
            // and never exceed the input maximum.
            var watermarks = results.Where(static r => r.IsT2).Select(static r => r.AsT2).ToArray();
            var real = watermarks.Where(static w => !w.IsNothing).ToArray();
            Assert.NotEmpty(real);
            Assert.DoesNotContain(watermarks, static w => w.IsNothing);
            AssertX.StrictlyIncreasing(real);
            Assert.All(real, w => Assert.True(w <= Watermark.From((n / 3) * 10)));

            // The final (global progress) watermark is exactly the last of the input.
            Assert.True(results[^1].IsT2, "watermark should be the last item");
            Assert.Equal(Watermark.From((n / 3) * 10), results[^1].AsT2);
        }

        [Fact(Timeout = 1_000)]
        public async Task MonotonicWatermarks_UniqueKeys_LargeInput_LastWatermarkEmitted()
        {
            // 500 unique warmable keys, the watermark grows but repeats three times (as a real provider
            // would within one clock tick). The final output watermark must be the last (maximum),
            // not the first or an intermediate one.
            const int n = 500;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i * 2, Watermark.From(((i + 1) / 3) * 10)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // warm even keys

            var results = await RunAsync(
                new SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                null,
                TestContext.Current.CancellationToken);

            Assert.DoesNotContain(results, static r => r.IsT0);

            // Each position is a separate group (in segment order).
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();
            Assert.Equal(n, groups.Length);

            // Real (non-Nothing) output watermarks are non-decreasing. With repeated input watermarks the
            // same value may legitimately appear more than once (it lands in both the closing and the next
            // window), so collapse consecutive duplicates and require the remaining distinct sequence to be
            // strictly increasing — progress never goes backwards.
            var watermarks = results.Where(static r => r.IsT2).Select(static r => r.AsT2).ToArray();
            var real = watermarks.Where(static w => !w.IsNothing).ToArray();
            Assert.NotEmpty(real);
            Assert.DoesNotContain(watermarks, static w => w.IsNothing);
            AssertX.StrictlyIncreasing(real);

            // The maximum real watermark equals the maximum of the input.
            var maxInput = Watermark.From((n / 3) * 10);
            Assert.Equal(maxInput, real.Max());
        }
    }
}