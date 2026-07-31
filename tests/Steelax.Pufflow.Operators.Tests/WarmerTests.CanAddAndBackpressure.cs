namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmerTests
{
    public sealed class CanAddAndBackpressure
    {
        [Fact]
        public void InitiallyTrue()
        {
            using var warmer = Create();
            Assert.True(warmer.CanAdd);
        }

        [Fact]
        public void PartialTail_RemainsAddable()
        {
            using var warmer = Create(segmentCapacity: 5);

            AddKeys(warmer, (1, 10), (2, 20));

            Assert.True(warmer.CanAdd);
        }

        [Fact]
        public void FullRingWithFullTail_Backpressures()
        {
            using var warmer = Create(maxConcurrency: 1, maxQueued: 2, segmentCapacity: 2);

            AddKeys(warmer, (1, 10), (2, 20), (3, 30), (4, 40));

            Assert.False(warmer.CanAdd);
        }

        [Fact]
        public void WarmNext_DrainsAndRestoresCanAdd()
        {
            using var warmer = Create(maxConcurrency: 1, maxQueued: 2, segmentCapacity: 2);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10), (2, 20), (3, 30), (4, 40));
            Assert.False(warmer.CanAdd);

            Assert.True(warmer.WarmNext(sink, out _, out _));
            Assert.True(warmer.CanAdd);
        }
    }
}