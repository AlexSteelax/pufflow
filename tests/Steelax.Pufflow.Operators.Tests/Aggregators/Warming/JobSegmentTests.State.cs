using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

/// <summary>Tests for the derived state flags of <see cref="JobSegment{TKey,TWarm}" />.</summary>
public static partial class JobSegmentTests
{
    public sealed class State
    {
        [Fact]
        public void Initially_CanAdvanceAndEmpty()
        {
            var segment = new JobSegment<int, string>(capacity: 2);

            Assert.True(segment.CanAdvance);
            Assert.False(segment.HasAny);
            Assert.False(segment.IsJobAssigned);
            Assert.False(segment.IsJobCompleted);
        }

        [Fact]
        public void AfterAdvance_HasAny_AndStillCanAdvance()
        {
            var segment = new JobSegment<int, string>(capacity: 2);

            segment.Advance(1, Watermark.From(10));

            Assert.True(segment.HasAny);
            Assert.True(segment.CanAdvance);
            Assert.False(segment.IsJobAssigned);
        }

        [Fact]
        public void AfterRunJob_Completed_NotCanAdvance()
        {
            var segment = new JobSegment<int, string>(capacity: 2);

            segment.Advance(1, Watermark.From(10));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);

            Assert.True(segment.IsJobAssigned);
            Assert.True(segment.IsJobCompleted);
            Assert.False(segment.CanAdvance);
        }

        [Fact]
        public void AfterApply_ResetToInitial()
        {
            var segment = new JobSegment<int, string>(capacity: 2);

            segment.Advance(1, Watermark.From(10));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);
            segment.ApplyResult(new WarmingHelper.DefaultPolicy());

            // After ApplyResult the segment is recycled: empty, job/task detached and ready to accept keys.
            Assert.True(segment.CanAdvance);
            Assert.False(segment.HasAny);
            Assert.False(segment.IsJobAssigned);
            Assert.False(segment.IsJobCompleted);
        }
    }
}

