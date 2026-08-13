using Steelax.Pufflow.Operators.Aggregators.Chunking;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

public static partial class ChunkProcessorTests
{
    public sealed class Constructor
    {
        [Fact]
        public void NullChunker_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new ChunkProcessor<int, Chunk<int>>(null!, 3, TimeSpan.FromSeconds(1)));
        }

        [Fact]
        public void NonPositiveSize_ThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new ChunkProcessor<int, Chunk<int>>(new Chunker<int>(), 0, TimeSpan.FromSeconds(1)));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new ChunkProcessor<int, Chunk<int>>(new Chunker<int>(), -1, TimeSpan.FromSeconds(1)));
        }

        [Fact]
        public void NonPositiveLinger_ThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new ChunkProcessor<int, Chunk<int>>(new Chunker<int>(), 3, TimeSpan.Zero));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new ChunkProcessor<int, Chunk<int>>(new Chunker<int>(), 3, TimeSpan.FromMilliseconds(-1)));
        }

        [Fact]
        public void ValidArguments_DoesNotThrow()
        {
            var processor = new ChunkProcessor<int, Chunk<int>>(new Chunker<int>(), 3, TimeSpan.FromSeconds(5));
            Assert.NotNull(processor);
        }
    }
}
