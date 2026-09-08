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
            // break correctness вЂ” all records are delivered and the stream completes.
            const int n = 200;
            var input = Enumerable.Range(0, n)
                .Select(i => new Carrier<int>(i, Watermark.From(i)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(WarmEvenOnly); // warm even keys

            var results = await RunAsync(
                new WarmingHelper.DelayedJobFactory(2),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                DefaultOptions(TimeSpan.FromMilliseconds(10)),
                TestContext.Current.CancellationToken);

            var values = Values(results);
            var groups = Groups(results);

            Assert.Equal(n / 2, values.Length);
            Assert.Equal(n / 2, groups.Length);
            Assert.Equal(n, values.Length + groups.Length);
            Assert.Contains(results, static r => !r.HasValue);
            Assert.Equal(n / 2, policy.PlainItems.Count);
        }

        [Fact(Timeout = 1_000)]
        public async Task DisabledByDefault_CompletesImmediately()
        {
            // watchdogPeriod not passed в†’ null в†’ watchdog disabled (as before).
            await using var flow = new FlowSource();

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                new WarmingHelper.PredicatePolicy(WarmEvenOnly),
                new ListAccumulatorFactory(),
                [],
                flow,
                DefaultOptions(),
                TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }

        [Fact(Timeout = 1_000)]
        public async Task InfinitePeriod_Disabled_CompletesImmediately()
        {
            // Explicit Timeout.InfiniteTimeSpan в†’ watchdog disabled.
            await using var flow = new FlowSource();

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                new WarmingHelper.PredicatePolicy(WarmEvenOnly),
                new ListAccumulatorFactory(),
                [],
                flow,
                DefaultOptions(Timeout.InfiniteTimeSpan),
                TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }
    }
}

