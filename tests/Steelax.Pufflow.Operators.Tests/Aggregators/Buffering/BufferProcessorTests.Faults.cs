namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

public static partial class BufferProcessorTests
{
    public sealed class Faults
    {
        [Fact]
        public async Task FaultedSource_RethrowsCompletionErrorOnConsumption()
        {
            // Items already written before the fault are delivered, then the completion error supplied by
            // the push side is rethrown to the pull-side reader (not silently swallowed as end-of-stream).
            var ex = new InvalidOperationException("source error");
            var (flow, writer, reader) = BuildPipeline(capacity: 4);

            writer.TryWrite(1);
            writer.TryWrite(2);
            writer.TryComplete(ex);

            await using var _ = flow;
            await flow.ExecuteAsync(TestContext.Current.CancellationToken);

            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                    .ToListAsync(TestContext.Current.CancellationToken);
            });

            Assert.Same(ex, thrown);
        }
    }
}