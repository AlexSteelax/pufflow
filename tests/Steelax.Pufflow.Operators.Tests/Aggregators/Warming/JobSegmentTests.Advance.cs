using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

/// <summary>Tests for <see cref="JobSegment{TKey,TWarm}.Advance(TKey,Watermark)" /> and watermark folding.</summary>
public static partial class JobSegmentTests
{
    public sealed class Advance
    {
        [Fact]
        public void KeepsMaximumWatermark()
        {
            var segment = new JobSegment<int, string>(capacity: 4);

            segment.Advance(1, Watermark.From(10));
            segment.Advance(2, Watermark.From(30));
            segment.Advance(3, Watermark.From(20));

            Assert.Equal(Watermark.From(30), segment.Watermark);
        }

        [Fact]
        public void AfterRun_ThrowsInvalidOperation()
        {
            var segment = new JobSegment<int, string>(capacity: 2);

            segment.Advance(1, Watermark.From(10));
            segment.RunJob(new WarmingHelper.PartialJobFactory([]), CancellationToken.None);

            Assert.Throws<InvalidOperationException>(() => segment.Advance(2, Watermark.From(20)));
        }

        [Fact]
        public void WhenFull_ThrowsInvalidOperation()
        {
            var segment = new JobSegment<int, string>(capacity: 1);

            segment.Advance(1, Watermark.From(10));

            Assert.Throws<InvalidOperationException>(() => segment.Advance(2, Watermark.From(20)));
        }
    }
}