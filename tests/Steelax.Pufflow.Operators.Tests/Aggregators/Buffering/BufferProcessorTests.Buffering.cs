namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

public static partial class BufferProcessorTests
{
    public sealed class Buffering
    {
        [Fact]
        public async Task FastSource_YieldsAllItemsInOrder()
        {
            var result = await CollectAsync(capacity: 2, Enumerable.Range(1, 5));

            Assert.Equal(Enumerable.Range(1, 5), result);
        }

        [Fact]
        public async Task EmptySource_YieldsNothing()
        {
            var result = await CollectAsync(capacity: 2, Array.Empty<int>());

            Assert.Empty(result);
        }

        [Fact]
        public async Task SourceLargerThanBuffer_AllItemsDelivered()
        {
            // The writer blocks on the full buffer (backpressure) and resumes as the reader drains it.
            var result = await CollectAsync(capacity: 2, Enumerable.Range(0, 100));

            Assert.Equal(Enumerable.Range(0, 100), result);
        }

        [Fact]
        public async Task SlowConsumer_ReceivesAllItemsWithoutLoss()
        {
            var (flow, writer, reader) = BuildPipeline(capacity: 2);

            foreach (var item in Enumerable.Range(0, 20))
                writer.TryWrite(item);

            writer.TryComplete();

            await using var _ = flow;
            var execute = flow.ExecuteAsync(TestContext.Current.CancellationToken);

            var result = new List<int>();
            await foreach (var item in reader.ReadAllAsync(TestContext.Current.CancellationToken))
            {
                result.Add(item);
                await Task.Delay(5, TestContext.Current.CancellationToken);
            }

            await execute;

            Assert.Equal(Enumerable.Range(0, 20), result);
        }
    }
}