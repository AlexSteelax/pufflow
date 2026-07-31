namespace Steelax.Pufflow.Operators.Tests;

public static partial class ChunkProcessorTests
{
    public sealed class CountChunking
    {
        [Fact]
        public async Task FillsBySize_FlushesTrailingOnEof()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 2, TimeSpan.FromSeconds(5));
            var chunks = await CollectAsync(processor, new[] { 1, 2, 3, 4, 5 }.ToAsyncEnumerable());

            AssertChunks(chunks, [1, 2], [3, 4], [5]);
        }

        [Fact]
        public async Task ExactFill_FlushesImmediately()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 3, TimeSpan.FromSeconds(5));
            var chunks = await CollectAsync(processor, new[] { 1, 2, 3 }.ToAsyncEnumerable());

            AssertChunks(chunks, [1, 2, 3]);
        }

        [Fact]
        public async Task EmptySource_YieldsNoChunks()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 3, TimeSpan.FromSeconds(5));
            var chunks = await CollectAsync(processor, Array.Empty<int>().ToAsyncEnumerable());

            Assert.Empty(chunks);
        }
    }
}