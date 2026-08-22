namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

public static partial class ChunkProcessorTests
{
    public sealed class CountChunking
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task FillsBySize_FlushesTrailingOnEof()
        {
            await using var flow = new FlowSource();
            var chunks = await RunAsync([1, 2, 3, 4, 5], size: 2, TimeSpan.FromSeconds(5), flow, TestContext.Current.CancellationToken);

            AssertChunks(chunks, [1, 2], [3, 4], [5]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task ExactFill_FlushesImmediately()
        {
            await using var flow = new FlowSource();
            var chunks = await RunAsync([1, 2, 3], size: 3, TimeSpan.FromSeconds(5), flow, TestContext.Current.CancellationToken);

            AssertChunks(chunks, [1, 2, 3]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task EmptySource_YieldsNoChunks()
        {
            await using var flow = new FlowSource();
            var chunks = await RunAsync([], size: 3, TimeSpan.FromSeconds(5), flow, TestContext.Current.CancellationToken);

            Assert.Empty(chunks);
        }
    }
}
