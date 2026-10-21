using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Internal;

namespace Steelax.Pufflow.Operators.Tests.Internal;

/// <summary>
///     Tests for <see cref="WatermarkBarrier" /> round semantics: how the barrier gathers one watermark
///     contribution per source, computes their minimum, and releases it once a round is complete.
/// </summary>
public static partial class WatermarkBarrierTests
{
    public sealed class Rounds
    {
        /// <summary>
        ///     After every source contributed in the current round, <see cref="WatermarkBarrier.NextRound" />
        ///     must release the minimum of the per-source watermarks and re-arm the round. This also implies the
        ///     first contribution of a round seeds the barrier (there is no smaller value to compare against yet).
        /// </summary>
        [Fact]
        public void TryReset_WhenRoundComplete_ReleasesMinimum()
        {
            var barrier = new WatermarkBarrier(2);

            barrier.Set(0, Watermark.From(100));
            barrier.Set(1, Watermark.From(200));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal(Watermark.From(100), watermark);
            Assert.False(barrier.IsCompleted);
        }

        /// <summary>
        ///     The minimum is order-independent: the last contribution may also be the smallest one.
        /// </summary>
        [Fact]
        public void TryReset_WhenSmallestContributesLast_KeepsIt()
        {
            var barrier = new WatermarkBarrier(3);

            barrier.Set(0, Watermark.From(300));
            barrier.Set(1, Watermark.From(200));
            barrier.Set(2, Watermark.From(100));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal(Watermark.From(100), watermark);
        }

        /// <summary>
        ///     <see cref="WatermarkBarrier.NextRound" /> must not fire before every source has contributed in
        ///     the current round.
        /// </summary>
        [Fact]
        public void TryReset_WhenRoundIncomplete_ReturnsFalse()
        {
            var barrier = new WatermarkBarrier(3);

            barrier.Set(0, Watermark.From(100));
            barrier.Set(1, Watermark.From(150));

            Assert.False(barrier.NextRound(out _));
        }

        /// <summary>
        ///     A source that completes without contributing must not block the round: the barrier releases the
        ///     minimum over the remaining live sources. Completing a single source while others are alive is not
        ///     the end of the merged stream.
        /// </summary>
        [Fact]
        public void TryReset_WhenOneSourceCompleted_StillReleasesOverLiveSources()
        {
            var barrier = new WatermarkBarrier(3);

            barrier.Complete(0);
            barrier.Set(1, Watermark.From(100));
            barrier.Set(2, Watermark.From(200));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal(Watermark.From(100), watermark);
            Assert.False(barrier.IsCompleted);
        }

        /// <summary>
        ///     A redundant completion (source already marked) is idempotent and does not corrupt the barrier.
        /// </summary>
        [Fact]
        public void Complete_WhenAlreadyCompleted_IsIdempotent()
        {
            var barrier = new WatermarkBarrier(2);

            barrier.Complete(0);
            barrier.Complete(0);
            barrier.Set(1, Watermark.From(150));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal(Watermark.From(150), watermark);
        }

        /// <summary>
        ///     The barrier value is reported in the same logical value space it was fed with
        ///     (<see cref="Watermark.From" /> must not double-apply the internal bias).
        /// </summary>
        [Fact]
        public void Set_ThenComplete_ReportsSameLogicalValue()
        {
            var barrier = new WatermarkBarrier(2);

            barrier.Set(0, Watermark.From(1000));
            barrier.Set(1, Watermark.From(1200));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal((long)Watermark.From(1000), watermark);
        }
    }
}