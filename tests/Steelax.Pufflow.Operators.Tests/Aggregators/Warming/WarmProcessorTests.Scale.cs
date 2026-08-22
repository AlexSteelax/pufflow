using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class Scale
    {
        [Fact(Timeout = 5_000)]
        public async Task LargeInput_WithDelayedWarmJobs_AllDelivered()
        {
            // Large stream: each key appears once, even keys are warmed (delayed job),
            // odd keys pass through. Verifies no loss and correct output.
            const int n = 1_000;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i, Watermark.From(i)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // warm even keys

            var results = await RunAsync(
                new DelayedJobFactory(15),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                null,
                TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();

            // Odd keys pass through (in source order), even keys are groups (in segment order).
            Assert.Equal(
                Enumerable.Range(0, n).Where(static i => i % 2 != 0),
                values);
            Assert.Equal(
                Enumerable.Range(0, n).Where(static i => i % 2 == 0).Select(static i => i.ToString()),
                groups);

            Assert.Contains(results, static r => r.IsT2);
            Assert.Equal(n / 2, policy.Warmed.Count);
        }

        [Fact(Timeout = 1_000)]
        public async Task LargeInput_SyncJobs_SameCountOnOutput()
        {
            // Long distance: the output has exactly as many useful records (T0 passthrough +
            // T1 groups) as were fed in — nothing is lost or duplicated.
            const int n = 1_000;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i, Watermark.From(i)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // warm even keys, odd ones pass through

            var results = await RunAsync(
                new SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                null,
                TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();

            // Each even key produced a group, each odd key passed through; nothing was lost.
            Assert.Equal(n / 2, values.Length);
            Assert.Equal(n / 2, groups.Length);
            Assert.Equal(n, values.Length + groups.Length);

            // Passthrough order is preserved.
            Assert.Equal(Enumerable.Range(0, n).Where(static i => i % 2 != 0), values);

            // Groups were emitted in segment order.
            Assert.Equal(
                Enumerable.Range(0, n).Where(static i => i % 2 == 0).Select(static i => i.ToString()),
                groups);

            Assert.Contains(results, static r => r.IsT2);
            Assert.Equal(n / 2, policy.Warmed.Count);
        }
    }
}