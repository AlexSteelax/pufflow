namespace Steelax.Pufflow.Operators.Tests;

public static partial class ChunkProcessorTests
{
    public sealed class Faults
    {
        [Fact]
        public async Task FaultedSource_DeliversAccumulatedChunk_ThenThrows()
        {
            var ex = new InvalidOperationException("source error");
            var processor = new ChunkProcessor<int>(new Chunker<int>(), 10, TimeSpan.FromSeconds(1));

            await using var sourceEnumerator =
                FaultySourceAsync(ex).GetAsyncEnumerator(TestContext.Current.CancellationToken);
            await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

            // The accumulated chunk is delivered before the source fault propagates.
            Assert.True(await enumerator.MoveNextAsync());
            Assert.Equal([1], enumerator.Current.Span.ToArray());
            enumerator.Current.Dispose();

            var thrown =
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync());
            Assert.Same(ex, thrown);
        }
    }
}