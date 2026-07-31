namespace Steelax.Pufflow.Operators.Tests;

public static partial class BufferProcessorTests
{
    public sealed class Constructor
    {
        [Fact]
        public void NullBuffer_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new BufferProcessor<int>(0));
        }

        [Fact]
        public void ValidArguments_DoesNotThrow()
        {
            var processor = new BufferProcessor<int>(4);
            Assert.NotNull(processor);
        }
    }
}