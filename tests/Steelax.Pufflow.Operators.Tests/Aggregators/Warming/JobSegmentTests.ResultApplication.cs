using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

/// <summary>Tests for <see cref="JobSegment{TKey,TWarm}.ApplyResult" /> result dispatch.</summary>
public static partial class JobSegmentTests
{
    public sealed class ResultApplication
    {
        [Fact]
        public void WhenJobWritesEveryKey_NotifiesPolicyWithWarm()
        {
            var segment = new JobSegment<int, string>(capacity: 4);
            var policy = new WarmingHelper.DefaultPolicy();

            segment.Advance(1, Watermark.From(10));
            segment.Advance(2, Watermark.From(20));
            segment.RunJob(new WarmingHelper.SyncJobFactory(), CancellationToken.None);

            var keys = segment.ApplyResult(policy);

            Assert.Equal(new[] { 1, 2 }, keys);
            Assert.Equal([(1, "W1", true), (2, "W2", true)], policy.PlainItems);
        }

        [Fact]
        public void WhenJobWritesNoResult_NotifiesPolicyWithoutWarm()
        {
            var segment = new JobSegment<int, string>(capacity: 4);
            var policy = new WarmingHelper.DefaultPolicy();

            segment.Advance(1, Watermark.From(10));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);

            var keys = segment.ApplyResult(policy);

            Assert.Equal(new[] { 1 }, keys);
            Assert.Equal([(1, "", false)], policy.PlainItems);
        }

        [Fact]
        public void WhenJobWritesSubset_NotifiesEveryKeyExactlyOnce()
        {
            var segment = new JobSegment<int, string>(capacity: 4);
            var policy = new WarmingHelper.DefaultPolicy();

            segment.Advance(1, Watermark.From(10));
            segment.Advance(2, Watermark.From(20));
            segment.Advance(3, Watermark.From(30));
            segment.RunJob(new WarmingHelper.PartialJobFactory([1, 3]), CancellationToken.None);

            var keys = segment.ApplyResult(policy);

            Assert.Equal(new[] { 1, 2, 3 }, keys);
            Assert.Equal([(1, "W1", true), (2, "", false), (3, "W3", true)], policy.PlainItems);
        }

        [Fact]
        public void ReturnsKeysInSegmentOrder()
        {
            var segment = new JobSegment<int, string>(capacity: 4);

            segment.Advance(3, Watermark.From(30));
            segment.Advance(1, Watermark.From(10));
            segment.Advance(2, Watermark.From(20));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);

            var keys = segment.ApplyResult(new WarmingHelper.DefaultPolicy());

            Assert.Equal(new[] { 3, 1, 2 }, keys);
        }

        [Fact]
        public void BeforeRun_ThrowsInvalidOperation()
        {
            var segment = new JobSegment<int, string>(capacity: 4);
            segment.Advance(1, Watermark.From(10));

            Assert.Throws<InvalidOperationException>(() => segment.ApplyResult(new WarmingHelper.DefaultPolicy()));
        }

        [Fact]
        public void Twice_ThrowsInvalidOperation()
        {
            var segment = new JobSegment<int, string>(capacity: 2);
            segment.Advance(1, Watermark.From(10));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);
            segment.ApplyResult(new WarmingHelper.DefaultPolicy());

            Assert.Throws<InvalidOperationException>(() => segment.ApplyResult(new WarmingHelper.DefaultPolicy()));
        }

        [Fact]
        public void FaultingJob_ApplyResultRethrowsAndGuards()
        {
            var segment = new JobSegment<int, string>(capacity: 2);
            segment.Advance(1, Watermark.From(10));

            // The faulting job surfaces its exception when the (completed) task is awaited in ApplyResult.
            segment.RunJob(new WarmingHelper.FaultingJobFactory(), CancellationToken.None);

            var ex = Assert.Throws<InvalidOperationException>(() => segment.ApplyResult(new WarmingHelper.DefaultPolicy()));
            Assert.Equal("boom", ex.Message);

            // ApplyResult reset the segment via finally; a second apply is guarded, not an NPE.
            Assert.Throws<InvalidOperationException>(() => segment.ApplyResult(new WarmingHelper.DefaultPolicy()));
        }
    }
}