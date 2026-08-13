using Steelax.Pufflow.Bridges;

namespace Steelax.Pufflow.Tests.Bridges;

public static partial class AsyncProducatorToAsyncEnumeratorTests
{
    /// <summary>
    ///     A single concurrent SPSC producer/consumer test exercising the tightest backpressure path
    ///     (limit = 1): every write blocks until the consumer drains the slot.
    /// </summary>
    public sealed class Debug
    {
        [Fact(Timeout = 10000)]
        public async Task Debug_ConcurrentProducerConsumer_SmallLimit()
        {
            const int count = 5000;
            const int limit = 1;
            var bridge = new AsyncProducatorToAsyncEnumerator<int>(limit);

            var producer = Task.Run(async () =>
            {
                for (var i = 0; i < count; i++)
                    while (!bridge.TryWrite(i))
                        await bridge.WaitToWriteAsync();

                bridge.Complete();
            }, TestContext.Current.CancellationToken);

            var read = 0;
            long sum = 0;

            while (await bridge.MoveNextAsync())
            {
                sum += bridge.Current;
                read++;
            }

            await producer;

            Assert.Equal(count, read);
            Assert.Equal((long)count * (count - 1) / 2, sum);
        }
    }
}