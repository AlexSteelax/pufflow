using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class Passthrough
    {
        [Fact(Timeout = 1_000)]
        public async Task WarmableAll_ReleaseValuesInOrder_ThenFinalProgressWatermark()
        {
            // A full warmed batch (SegmentCapacity = 4 keys): every key is warmable, so no value passes
            // through directly — each is released as its own accumulated group, and the batch's covering
            // watermark (60) is emitted at the end as the progress marker.
            var input = new List<Carrier<int>>
            {
                new(1, Watermark.From(10)),
                new(3, Watermark.From(30)),
                new(5, Watermark.From(50)),
                new(6, Watermark.From(60))
            };

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(static _ => true);

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                policy,
                new QueueAccumulatorFactory(),
                input,
                flow,
                DefaultOptions(),
                TestContext.Current.CancellationToken);

            // The warmed values come out as individual groups (queue accumulator), strictly in order.
            Assert.Equal(new[] { 1, 3, 5, 6 }, Groups(results));
            Assert.Empty(Values(results));

            // The covering watermark of the released batch is emitted at the end as the progress marker.
            Assert.Equal(new[] { Watermark.From(60) }, Progress(results));
            Assert.True(IsLastProgress(results), "watermark should be the last item");
        }

        [Fact(Timeout = 1_000)]
        public async Task WarmableNever_AllValuesPassThrough_NoGroups()
        {
            var input = new List<Carrier<int>>
            {
                new(1, Watermark.From(10)),
                new(3, Watermark.From(30)),
                new(5, Watermark.From(50))
            };

            await using var flow = new FlowSource();
            // Nothing is ever warmable — every value must pass straight through, no groups, no warming.
            var policy = new WarmingHelper.PredicatePolicy(static _ => false);

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                DefaultOptions(),
                TestContext.Current.CancellationToken);

            Assert.Equal(new[] { 1, 3, 5 }, Values(results));
            Assert.Equal(new[] { Watermark.From(10), Watermark.From(30), Watermark.From(50) }, Progress(results));
            Assert.Empty(Groups(results));
        }

        [Fact(Timeout = 2_000)]
        public async Task WarmablePartial_ReleaseInOrder_WhenTtlElapses()
        {
            // Only three values (SegmentCapacity = 4): a partial segment that is never filled by size.
            // It must be sealed and released by the segment TTL, not hang until the warm batch completes.
            var input = new List<Carrier<int>>
            {
                new(1, Watermark.From(10)),
                new(3, Watermark.From(30)),
                new(5, Watermark.From(50))
            };

            var options = DefaultOptions() with { SegmentTtl = TimeSpan.FromMilliseconds(50) };

            await using var flow = new FlowSource();
            var policy = new WarmingHelper.PredicatePolicy(static _ => true);

            var results = await RunAsync(
                new WarmingHelper.SyncJobFactory(),
                policy,
                new QueueAccumulatorFactory(),
                input,
                flow,
                options,
                TestContext.Current.CancellationToken);

            Assert.Equal(new[] { 1, 3, 5 }, Groups(results));
            Assert.Empty(Values(results));
            Assert.Equal(new[] { Watermark.From(50) }, Progress(results));
            Assert.True(IsLastProgress(results), "watermark should be the last item");
        }
    }
}

