using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Internal;

namespace Steelax.Pufflow.Operators.Tests.Internal;

/// <summary>
///     Tests for <see cref="WatermarkBarrier" /> completion semantics: how completed sources are excluded
///     from the gathered minimum, when the whole barrier is considered finished, and how the completion
///     flag is reported by <see cref="WatermarkBarrier.NextRound" />.
/// </summary>
public static partial class WatermarkBarrierTests
{
    public sealed class Completion
    {
        /// <summary>
        ///     A source that completes while another live source has <b>not yet contributed</b> does not reset the
        ///     barrier to a stale default: the minimum is taken only over same-set live watermarks, and the
        ///     completed source no longer participates.
        /// </summary>
        [Fact]
        public void Complete_WhenOtherSourceNotYetSet_DoesNotCorruptBarrier()
        {
            var barrier = new WatermarkBarrier(2);

            barrier.Set(0, Watermark.From(100));
            barrier.Complete(0);
            barrier.Set(1, Watermark.From(300));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal(Watermark.From(300), watermark);
            Assert.False(barrier.IsCompleted);
        }

        /// <summary>
        ///     <see cref="WatermarkBarrier.NextRound" /> reports the merged stream finished
        ///     (<see cref="WatermarkBarrier.IsCompleted" />) only when <b>every</b> source has completed. The
        ///     final release then reports the <b>maximum</b> of the last watermarks — a last-flush point that
        ///     covers every completed source.
        /// </summary>
        [Fact]
        public void TryReset_WhenAllSourcesCompleted_ReportsCompleted()
        {
            var barrier = new WatermarkBarrier(2);

            barrier.Set(0, Watermark.From(100));
            barrier.Set(1, Watermark.From(200));
            barrier.Complete(0);
            barrier.Complete(1);

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal(Watermark.From(200), watermark);
            Assert.True(barrier.IsCompleted);
        }

        /// <summary>
        ///     <see cref="WatermarkBarrier.NextRound" /> must <b>not</b> report completion while any source is
        ///     still alive (a single completed input does not end the merged stream).
        /// </summary>
        [Fact]
        public void TryReset_WhenOneSourceRemainsAlive_DoesNotReportCompleted()
        {
            var barrier = new WatermarkBarrier(3);

            barrier.Set(0, Watermark.From(100));
            barrier.Set(1, Watermark.From(200));
            barrier.Complete(0);
            barrier.Complete(1);
            barrier.Set(2, Watermark.From(300));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.False(barrier.IsCompleted, "completion must require every source to be completed");
        }

        /// <summary>
        ///     A completed source no longer constrains the barrier: the minimum is taken over the live sources
        ///     only, so the merged progress keeps advancing with the remaining stream.
        /// </summary>
        [Fact]
        public void Complete_DoesNotConstrainBarrierWithItsLastWatermark()
        {
            var barrier = new WatermarkBarrier(2);

            barrier.Set(0, Watermark.From(100));
            barrier.Set(1, Watermark.From(200));
            barrier.Complete(0);

            // Second round: source 0 completed, source 1 contributes a fresher watermark.
            barrier.Set(1, Watermark.From(250));

            Assert.True(barrier.NextRound(out var watermark));
            Assert.Equal(Watermark.From(250), watermark);
            Assert.False(barrier.IsCompleted);
        }

        /// <summary>
        ///     Once <see cref="WatermarkBarrier.IsCompleted" /> is reported, the barrier keeps reporting the
        ///     finished flag and stays drained (it does not resurrect a new round).
        /// </summary>
        [Fact]
        public void IsCompleted_WhenAllSourcesCompleted_ReturnsTrue()
        {
            var barrier = new WatermarkBarrier(2);

            barrier.Complete(0);
            barrier.Complete(1);

            Assert.True(barrier.IsCompleted);
        }

        /// <summary>
        ///     <see cref="WatermarkBarrier.HasCompleted" /> reflects the per-source completion state.
        /// </summary>
        [Fact]
        public void HasCompleted_ReflectsPerSourceState()
        {
            var barrier = new WatermarkBarrier(3);

            Assert.False(barrier.HasCompleted(0));

            barrier.Complete(1);

            Assert.False(barrier.HasCompleted(0));
            Assert.True(barrier.HasCompleted(1));
            Assert.False(barrier.HasCompleted(2));
        }
    }
}