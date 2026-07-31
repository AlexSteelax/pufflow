using System.Diagnostics;
using System.Threading.Channels;

namespace Steelax.Pufflow.Operators.Tests;

public static partial class BufferProcessorTests
{
    public sealed class Concurrency(ITestOutputHelper output)
    {
        private static Channel<int> CreateChannel(int capacity, bool allowSynchronousContinuations)
        {
            return Channel.CreateBounded<int>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = allowSynchronousContinuations
            });
        }

        [Fact(Timeout = 10000)]
        public async Task ConcurrentProducerConsumer_SmallLimit_NoLoss()
        {
            const int count = 500;
            var channel = CreateChannel(1, true);

            var producer = Task.Run(async () =>
            {
                for (var i = 0; i < count; i++)
                    await channel.Writer.WriteAsync(i, TestContext.Current.CancellationToken);

                channel.Writer.Complete();
            }, TestContext.Current.CancellationToken);

            var processor = new BufferProcessor<int>(1);
            var collected = await CollectAsync(processor, channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken));

            await producer.WaitAsync(TimeSpan.FromSeconds(8), TestContext.Current.CancellationToken);

            Assert.Equal(count, collected.Count);
            Assert.Equal((long)count * (count - 1) / 2, collected.Sum(x => (long)x));
        }

        [Theory(Timeout = 10000)]
        [InlineData(1_000_000, 1, true)]
        [InlineData(100, 1, false)]
        [InlineData(1_000_000, 4, true)]
        [InlineData(1_000, 4, false)]
        [InlineData(1_000_000, 32, true)]
        [InlineData(1_000, 32, false)]
        [InlineData(1_000_000, 128, true)]
        [InlineData(1_000, 128, false)]
        public async Task ConcurrentProducerConsumer_InputMatchesOutput(int count, int capacity, bool allowSynchronousContinuations)
        {
            var watch = Stopwatch.StartNew();
            var channel = CreateChannel(capacity, allowSynchronousContinuations);

            var producer = Task.Factory.StartNew(async () =>
            {
                for (var i = 0; i < count; i++)
                    await channel.Writer.WriteAsync(i, TestContext.Current.CancellationToken);

                channel.Writer.Complete();
            }, TaskCreationOptions.LongRunning).Unwrap();

            var processor = new BufferProcessor<int>(capacity);

            try
            {
                var collected = await CollectAsync(processor, channel.Reader.ReadAllAsync(TestContext.Current.CancellationToken));

                await producer.WaitAsync(TestContext.Current.CancellationToken);

                Assert.Equal(count, collected.Count);
                Assert.Equal(Enumerable.Range(0, count), collected);
            }
            finally
            {
                watch.Stop();

                output.WriteLine(watch.ElapsedMilliseconds is var elapsed && elapsed != 0 ? $"Time elapsed: {1m * count / elapsed:F3} item/ms" : "Time elapsed: - item/ms");
            }
        }

        [Fact(Timeout = 1000)]
        public async Task ConcurrentFault_RethrownOnConsumer()
        {
            var channel = CreateChannel(4, true);
            var ex = new InvalidOperationException("producer failed");

            var consumer = Task.Run(async () =>
            {
                var processor = new BufferProcessor<int>(4);
                await using var sourceEnumerator = channel.Reader.ReadAllAsync().GetAsyncEnumerator(TestContext.Current.CancellationToken);
                await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

                var result = new List<int>();
                while (await enumerator.MoveNextAsync())
                    result.Add(enumerator.Current);

                return result;
            }, TestContext.Current.CancellationToken);

            await Task.Delay(50, TestContext.Current.CancellationToken);

            channel.Writer.Complete(ex);

            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await consumer);
            Assert.Same(ex, thrown);
        }

        [Fact(Timeout = 1000)]
        public async Task ConcurrentEmptyComplete_ConsumerSeesEndOfStream()
        {
            var channel = CreateChannel(4, true);

            var consumer = Task.Run(async () =>
            {
                var processor = new BufferProcessor<int>(4);
                return await CollectAsync(processor, channel.Reader.ReadAllAsync());
            }, TestContext.Current.CancellationToken);

            await Task.Delay(50, TestContext.Current.CancellationToken);

            channel.Writer.Complete();

            var result = await consumer;
            Assert.Empty(result);
        }
    }
}
