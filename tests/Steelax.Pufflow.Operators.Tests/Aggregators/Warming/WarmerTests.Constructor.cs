using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    public sealed class Constructor
    {
        [Fact]
        public void NullJobFactory_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new Warmer<int, string>(1, 1, 1, null!));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(33)]
        public void InvalidMaxConcurrency_ThrowsArgumentOutOfRangeException(int maxConcurrency)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new Warmer<int, string>(maxConcurrency, 1, 1, new WarmingHelper.SyncJobFactory()));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void InvalidMaxQueued_ThrowsArgumentOutOfRangeException(int maxQueued)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new Warmer<int, string>(1, maxQueued, 1, new WarmingHelper.SyncJobFactory()));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void InvalidSegmentCapacity_ThrowsArgumentOutOfRangeException(int segmentCapacity)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new Warmer<int, string>(1, 1, segmentCapacity, new WarmingHelper.SyncJobFactory()));
        }

        [Fact]
        public async Task ValidArguments_DoesNotThrow()
        {
            await using var warmer = new Warmer<int, string>(2, 4, 2, new WarmingHelper.SyncJobFactory());
            Assert.NotNull(warmer);
        }
    }
}
