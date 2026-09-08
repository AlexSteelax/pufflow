using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    public sealed class Warming
    {
        [Fact]
        public async Task Fill_StartsJobSynchronouslyAndSignals()
        {
            var callback = new WarmingHelper.RecordingCallback();
            await using var warmer = Create(onReady: callback.Invoke, segmentCapacity: 2);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2]);

            // The segment is full — the job started synchronously and raised the readiness signal.
            Assert.True(callback.Count > 0);

            Assert.True(warmer.WarmNext(sink, WarmMode.Normal, out var keys, out var watermark));

            Assert.Equal(new[] { 1, 2 }, keys);
            Assert.False(watermark.IsNothing);
            Assert.Equal(2, sink.Items.Count);
            Assert.Equal("W1", sink.Items[1].Result);
            Assert.Equal("W2", sink.Items[2].Result);
        }

        [Fact]
        public async Task PartialSegment_NotSealedInNormalMode()
        {
            var factory = new WarmingHelper.SyncJobFactory();
            await using var warmer = Create(factory, segmentCapacity: 5);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2]);

            // A partial segment is not started in Normal mode — nothing is assigned or emitted.
            Assert.Equal(0, factory.CreatedCount);
            Assert.False(warmer.WarmNext(sink, WarmMode.Normal, out var keys, out var watermark));
            Assert.Null(keys);
            Assert.True(watermark.IsNothing);
            Assert.Empty(sink.Items);
            Assert.Equal(0, factory.CreatedCount);
        }

        [Fact]
        public async Task PartialSegment_SealedInSealTailMode()
        {
            var factory = new WarmingHelper.SyncJobFactory();
            await using var warmer = Create(factory, segmentCapacity: 5);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2]);

            Assert.True(warmer.WarmNext(sink, WarmMode.SealTail, out var keys, out var watermark));
            Assert.Equal(new[] { 1, 2 }, keys);
            Assert.False(watermark.IsNothing);
            Assert.Equal(2, sink.Items.Count);
            Assert.Equal("W1", sink.Items[1].Result);
            Assert.Equal("W2", sink.Items[2].Result);
            Assert.Equal(1, factory.CreatedCount);
        }

        [Fact]
        public async Task NoCompletedHead_ReturnsFalse()
        {
            await using var warmer = Create(segmentCapacity: 5);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1]);

            Assert.False(warmer.WarmNext(sink, WarmMode.Normal, out var keys, out var watermark));
            Assert.Null(keys);
            Assert.True(watermark.IsNothing);
            Assert.Empty(sink.Items);
        }

        [Fact]
        public async Task Empty_ReturnsFalse()
        {
            await using var warmer = Create();
            var sink = new WarmingHelper.DefaultPolicy();

            Assert.False(warmer.WarmNext(sink, WarmMode.Normal, out var keys, out var watermark));
            Assert.Null(keys);
            Assert.True(watermark.IsNothing);
            Assert.Empty(sink.Items);
        }
    }
}