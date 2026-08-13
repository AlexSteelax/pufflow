namespace Steelax.Pufflow.Operators.Tests;

public static partial class ChunkProcessorTests
{
    public sealed class Linger
    {
        [Fact]
        public async Task PartialChunk_FlushedAfterLinger()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 100, TimeSpan.FromMilliseconds(30));
            var chunks = await CollectAsync(processor, DelayedSourceAsync(120, 1, 2, 3));

            // linger=30ms fires during each 120ms idle gap → each item is flushed on its own.
            AssertChunks(chunks, [1], [2], [3]);
        }

        [Fact]
        public async Task FastSource_FillsByCount_BeforeLinger()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 3, TimeSpan.FromSeconds(5));
            var chunks = await CollectAsync(processor, new[] { 1, 2, 3, 4, 5, 6 }.ToAsyncEnumerable());

            AssertChunks(chunks, [1, 2, 3], [4, 5, 6]);
        }

        [Fact]
        public async Task LingerFlush_ThenCountFlush_ContinuesOnFreshChunk()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 2, TimeSpan.FromMilliseconds(30));
            var chunks = await CollectAsync(processor, SegmentedSourceAsync((new[] { 1 }, 120), (new[] { 2, 3 }, 0)));

            // [1] is flushed by linger while the source is idle; the fresh chunk then
            // accumulates 2 and 3 and is flushed by the count trigger.
            AssertChunks(chunks, [1], [2, 3]);
        }

        [Fact]
        public async Task ItemsWithinWindow_AccumulateIntoOneChunk()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 10, TimeSpan.FromMilliseconds(30));
            var chunks = await CollectAsync(processor, SegmentedSourceAsync((new[] { 1, 2 }, 120), (new[] { 3 }, 0)));

            // Items arriving inside the linger window accumulate into a single chunk:
            // the window starts on the first element of the chunk, not on every element.
            AssertChunks(chunks, [1, 2], [3]);
        }

        [Fact]
        public async Task CountAndLinger_AlternateWithinSingleStream()
        {
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 2, TimeSpan.FromMilliseconds(30));
            var chunks = await CollectAsync(processor,
                SegmentedSourceAsync((new[] { 1, 2 }, 0), (new[] { 3 }, 120), (new[] { 4, 5 }, 0)));

            // [1,2] flushed by count → [3] partial flushed by linger → [4,5] flushed by count.
            AssertChunks(chunks, [1, 2], [3], [4, 5]);
        }
    }
}