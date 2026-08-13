namespace Steelax.Pufflow.Operators.Tests;

public static partial class BufferProcessorTests
{
    public sealed class Buffering
    {
        [Fact]
        public async Task FastSource_YieldsAllItemsInOrder()
        {
            var processor = new BufferProcessor<int>(2);
            var result = await CollectAsync(processor, new[] { 1, 2, 3, 4, 5 }.ToAsyncEnumerable());

            Assert.Equal(new[] { 1, 2, 3, 4, 5 }, result);
        }

        [Fact]
        public async Task EmptySource_YieldsNothing()
        {
            var processor = new BufferProcessor<int>(2);
            var result = await CollectAsync(processor, Array.Empty<int>().ToAsyncEnumerable());

            Assert.Empty(result);
        }

        [Fact]
        public async Task SourceLargerThanBuffer_AllItemsDelivered()
        {
            // The worker must block on a full buffer and resume as the consumer drains it.
            var processor = new BufferProcessor<int>(2);
            var result = await CollectAsync(processor, Enumerable.Range(0, 100).ToAsyncEnumerable());

            Assert.Equal(Enumerable.Range(0, 100), result);
        }

        [Fact]
        public async Task SlowConsumer_ReceivesAllItemsWithoutLoss()
        {
            var processor = new BufferProcessor<int>(2);

            await using var sourceEnumerator = Enumerable.Range(0, 20).ToAsyncEnumerable()
                .GetAsyncEnumerator(TestContext.Current.CancellationToken);
            await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

            var result = new List<int>();
            while (await enumerator.MoveNextAsync())
            {
                result.Add(enumerator.Current);
                await Task.Delay(5, TestContext.Current.CancellationToken); // slow consumer
            }

            Assert.Equal(Enumerable.Range(0, 20), result);
        }
    }
}