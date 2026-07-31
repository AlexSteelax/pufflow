namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmerTests
{
    public sealed class Constructor
    {
        [Fact]
        public void NullJobFactory_ThrowsArgumentNullException() =>
            Assert.Throws<ArgumentNullException>(() =>
                new Warmer<int, string>(1, 1, 1, TimeSpan.FromSeconds(1), null!, new ManualTimeProvider()));

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(33)]
        public void InvalidMaxConcurrency_ThrowsArgumentOutOfRangeException(int maxConcurrency) =>
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new Warmer<int, string>(maxConcurrency, 1, 1, TimeSpan.FromSeconds(1), new SyncJobFactory(), new ManualTimeProvider()));

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void InvalidMaxQueued_ThrowsArgumentOutOfRangeException(int maxQueued) =>
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new Warmer<int, string>(1, maxQueued, 1, TimeSpan.FromSeconds(1), new SyncJobFactory(), new ManualTimeProvider()));

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void InvalidSegmentCapacity_ThrowsArgumentOutOfRangeException(int segmentCapacity) =>
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new Warmer<int, string>(1, 1, segmentCapacity, TimeSpan.FromSeconds(1), new SyncJobFactory(), new ManualTimeProvider()));

        [Theory]
        [InlineData(0)]
        [InlineData(-100)]
        public void InvalidLinger_ThrowsArgumentOutOfRangeException(int lingerMs) =>
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new Warmer<int, string>(1, 1, 1, TimeSpan.FromMilliseconds(lingerMs), new SyncJobFactory(), new ManualTimeProvider()));
    }
}
