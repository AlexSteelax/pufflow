using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    public sealed class CanAddAndBackpressure
    {
        [Fact]
        public async Task InitiallyTrue()
        {
            await using var warmer = Create();
            Assert.True(warmer.CanAdd);
        }

        [Fact]
        public async Task PartialTail_RemainsAddable()
        {
            await using var warmer = Create(segmentCapacity: 5);

            AddKeys(warmer, [1, 2]);

            Assert.True(warmer.CanAdd);
        }

        [Fact]
        public async Task FullRingWithFullTail_Backpressure()
        {
            await using var warmer = Create(maxConcurrency: 1, maxQueued: 2, segmentCapacity: 2);

            AddKeys(warmer, [1, 2, 3, 4]);

            Assert.False(warmer.CanAdd);
        }

        [Fact]
        public async Task WarmNext_DrainsAndRestoresCanAdd()
        {
            await using var warmer = Create(maxConcurrency: 1, maxQueued: 2, segmentCapacity: 2);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2, 3, 4]);
            Assert.False(warmer.CanAdd);

            Assert.True(warmer.WarmNext(sink, WarmMode.Normal, out _, out _));
            Assert.True(warmer.CanAdd);
        }
    }
}