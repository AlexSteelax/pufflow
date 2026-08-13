using System.Threading.Channels;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

public static partial class ChunkProcessorTests
{
    public sealed class Linger
    {
        [Fact(Timeout = 10_000)]
        public async Task WriterWithSpinDelay_FlushesPartialChunksByLinger()
        {
            // A slow writer (a separate thread spinning between elements) keeps chunks below capacity:
            // the linger timer must flush partial chunks rather than waiting for full ones. At least one
            // chunk shorter than the requested size proves the linger trigger fired.
            const int chunkSize = 64;
            const int items = 256;

            await using var flow = new FlowSource(TestContext.Current.CancellationToken);
            flow
                .OnAsyncConsumatorSource(out ChannelWriter<int> writer)
                .Chunking(chunkSize, TimeSpan.FromMilliseconds(20))
                .Consume(out var reader);

            var runTask = flow.ExecuteAsync();

            // Write with a spin-wait delay on a separate thread to emulate a slow producer.
            var producer = Task.Run(() =>
            {
                var spinner = new SpinWait();
                for (var i = 0; i < items; i++)
                {
                    writer.TryWrite(i);
                    spinner.SpinOnce();
                }

                writer.TryComplete();
            }, TestContext.Current.CancellationToken);

            var chunks = new List<int[]>();
            await foreach (var chunk in reader.ReadAllAsync(TestContext.Current.CancellationToken))
            {
                using (chunk)
                    chunks.Add(chunk.Span.ToArray());
            }

            await producer;
            await runTask;

            // All elements are delivered in order.
            Assert.Equal(Enumerable.Range(0, items), chunks.SelectMany(static c => c));

            // The linger trigger fired: at least one chunk is shorter than the requested size.
            Assert.True(chunks.Count > items / chunkSize);
            Assert.Contains(chunks, static c => c.Length < chunkSize);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task PartialChunk_FlushedAfterLinger()
        {
            await using var flow = new FlowSource(TestContext.Current.CancellationToken);
            var chunks = await RunTimedAsync(
                flow,
                size: 100,
                TimeSpan.FromMilliseconds(30),
                writer => FillDelayedAsync(writer, gapMs: 120, 1, 2, 3));

            // linger=30ms fires during each 120ms idle gap → each item is flushed on its own.
            AssertChunks(chunks, [1], [2], [3]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task FastSource_FillsByCount_BeforeLinger()
        {
            await using var flow = new FlowSource(TestContext.Current.CancellationToken);
            var chunks = await RunAsync([1, 2, 3, 4, 5, 6], size: 3, TimeSpan.FromSeconds(5), flow);

            AssertChunks(chunks, [1, 2, 3], [4, 5, 6]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task LingerFlush_ThenCountFlush_ContinuesOnFreshChunk()
        {
            await using var flow = new FlowSource(TestContext.Current.CancellationToken);
            var chunks = await RunTimedAsync(
                flow,
                size: 2,
                TimeSpan.FromMilliseconds(30),
                writer => FillSegmentedAsync(writer, ([1], 120), ([2, 3], 0)));

            // [1] is flushed by linger while the source is idle; the fresh chunk then
            // accumulates 2 and 3 and is flushed by the count trigger.
            AssertChunks(chunks, [1], [2, 3]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task ItemsWithinWindow_AccumulateIntoOneChunk()
        {
            await using var flow = new FlowSource(TestContext.Current.CancellationToken);
            var chunks = await RunTimedAsync(
                flow,
                size: 10,
                TimeSpan.FromMilliseconds(30),
                writer => FillSegmentedAsync(writer, ([1, 2], 120), ([3], 0)));

            // Items arriving inside the linger window accumulate into a single chunk:
            // the window starts on the first element of the chunk, not on every element.
            AssertChunks(chunks, [1, 2], [3]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task CountAndLinger_AlternateWithinSingleStream()
        {
            await using var flow = new FlowSource(TestContext.Current.CancellationToken);
            var chunks = await RunTimedAsync(
                flow,
                size: 2,
                TimeSpan.FromMilliseconds(30),
                writer => FillSegmentedAsync(writer, ([1, 2], 0), ([3], 120), ([4, 5], 0)));

            // [1,2] flushed by count → [3] partial flushed by linger → [4,5] flushed by count.
            AssertChunks(chunks, [1, 2], [3], [4, 5]);
        }
    }
}
