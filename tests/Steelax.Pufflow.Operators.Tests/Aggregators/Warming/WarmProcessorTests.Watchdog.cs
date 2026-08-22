using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class Watchdog
    {
        [Fact(Timeout = 1_000)]
        public async Task Enabled_ShortPeriod_LongPipelineCompletes()
        {
            // Watchdog enabled with a short period: frequent spurious wake-ups must not
            // break correctness — all records are delivered and the stream completes.
            const int n = 500;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i, Watermark.From(i)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // warm even keys

            var results = await RunAsync(
                new DelayedJobFactory(2),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                TimeSpan.FromMilliseconds(10),
                TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();

            Assert.Equal(n / 2, values.Length);
            Assert.Equal(n / 2, groups.Length);
            Assert.Equal(n, values.Length + groups.Length);
            Assert.Contains(results, static r => r.IsT2);
            Assert.Equal(n / 2, policy.Warmed.Count);
        }

        [Fact(Timeout = 1_000)]
        public async Task DisabledByDefault_CompletesImmediately()
        {
            // watchdogPeriod not passed → null → watchdog disabled (as before).
            await using var flow = new FlowSource();

            var results = await RunAsync(
                new SyncJobFactory(),
                new TestPolicy(),
                new ListAccumulatorFactory(),
                [],
                flow,
                null,
                TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }

        [Fact(Timeout = 1_000)]
        public async Task InfinitePeriod_Disabled_CompletesImmediately()
        {
            // Explicit Timeout.InfiniteTimeSpan → watchdog disabled.
            await using var flow = new FlowSource();

            var results = await RunAsync(
                new SyncJobFactory(),
                new TestPolicy(),
                new ListAccumulatorFactory(),
                [],
                flow,
                Timeout.InfiniteTimeSpan,
                TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }
    }
}