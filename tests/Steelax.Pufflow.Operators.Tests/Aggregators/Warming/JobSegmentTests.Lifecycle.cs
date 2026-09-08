using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

/// <summary>Tests for the reuse/restart lifecycle of <see cref="JobSegment{TKey,TWarm}" />.</summary>
public static partial class JobSegmentTests
{
    public sealed class Lifecycle
    {
        [Fact]
        public void RunJob_AfterApply_CanRunAgain()
        {
            var segment = new JobSegment<int, string>(capacity: 2);
            var policy = new WarmingHelper.DefaultPolicy();

            segment.Advance(1, Watermark.From(10));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);
            segment.ApplyResult(policy);

            segment.Advance(2, Watermark.From(20));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);
            var keys = segment.ApplyResult(policy);

            Assert.Equal(new[] { 2 }, keys);
        }

        [Fact]
        public async Task RunJob_TwiceWithoutApply_ThrowsInvalidOperation()
        {
            var segment = new JobSegment<int, string>(capacity: 2);

            segment.Advance(1, Watermark.From(10));
            await segment.RunJob(new WarmingHelper.SyncJobFactory(), CancellationToken.None);

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                segment.RunJob(new WarmingHelper.SyncJobFactory(), CancellationToken.None));
        }
    }
}