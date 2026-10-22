using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class Warming
    {
        [Fact(Timeout = 1_000)]
        public async Task WarmableValues_ProduceGroups_AndWatermark()
        {
            var input = SeqValues(
                (2, 20),
                (4, 40));

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(WarmEvenOnly); // warm even keys (2, 4)

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                DefaultOptions() with { SegmentCapacity = 2 },
                TestContext.Current.CancellationToken);

            var groups = Groups(results);
            Assert.Equal(new[] { "2", "4" }, groups);

            Assert.Contains(results, static r => !r.HasValue);
            Assert.Equal(2, policy.PlainItems.Count);
        }

        [Fact(Timeout = 1_000)]
        public async Task Mixed_PassthroughAndWarmable_AllEmitted()
        {
            var input = SeqValues(
                (1, 10), // passthrough
                (2, 20), // warm
                (3, 30), // passthrough
                (4, 40)); // warm

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(WarmEvenOnly);

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                DefaultOptions() with { SegmentCapacity = 2 },
                TestContext.Current.CancellationToken);

            var values = Values(results);
            var groups = Groups(results);

            Assert.Equal(new[] { 1, 3 }, values);
            Assert.Equal(new[] { "2", "4" }, groups);
        }

        [Fact(Timeout = 1_000)]
        public async Task EmptySource_CompletesImmediately()
        {
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
    }
}

