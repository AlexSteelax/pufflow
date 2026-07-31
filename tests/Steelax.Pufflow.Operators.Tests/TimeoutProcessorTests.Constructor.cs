namespace Steelax.Pufflow.Operators.Tests;

public static partial class TimeoutProcessorTests
{
    public sealed class Constructor
    {
        [Fact]
        public void NonPositiveTimeout_ThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new TimeoutProcessor<int>(TimeSpan.Zero));
            Assert.Throws<ArgumentOutOfRangeException>(() => new TimeoutProcessor<int>(TimeSpan.FromMilliseconds(-1)));
        }

        [Fact]
        public void ValidArguments_DoesNotThrow()
        {
            var processor = new TimeoutProcessor<int>(TimeSpan.FromSeconds(5));
            Assert.NotNull(processor);
        }
    }
}