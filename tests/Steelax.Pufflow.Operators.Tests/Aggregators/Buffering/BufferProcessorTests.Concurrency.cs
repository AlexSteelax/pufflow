namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

public static partial class BufferProcessorTests
{
    public sealed class Concurrency
    {
        [Fact(Timeout = 10_000)]
        public async Task ConcurrentProducerConsumer_SmallLimit_NoLoss()
        {
            const int count = 500;
            var (flow, writer, reader) = BuildPipeline(capacity: 1);

            var producer = Task.Run(async () =>
            {
                for (var i = 0; i < count; i++)
                    await writer.WriteAsync(i, TestContext.Current.CancellationToken);

                writer.TryComplete();
            }, TestContext.Current.CancellationToken);

            await using var _ = flow;
            var execute = flow.ExecuteAsync(TestContext.Current.CancellationToken);

            var collected = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            await producer.WaitAsync(TestContext.Current.CancellationToken);
            await execute;

            Assert.Equal(count, collected.Count);
            Assert.Equal((long)count * (count - 1) / 2, collected.Sum(static x => (long)x));
        }

        [Fact(Timeout = 10_000)]
        public async Task ConcurrentProducerConsumer_InputMatchesOutput()
        {
            const int count = 1_000;
            var (flow, writer, reader) = BuildPipeline(capacity: 4);

            var producer = Task.Run(async () =>
            {
                for (var i = 0; i < count; i++)
                    await writer.WriteAsync(i, TestContext.Current.CancellationToken);

                writer.TryComplete();
            }, TestContext.Current.CancellationToken);

            await using var _ = flow;
            var execute = flow.ExecuteAsync(TestContext.Current.CancellationToken);

            var collected = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            await producer.WaitAsync(TestContext.Current.CancellationToken);
            await execute;

            Assert.Equal(count, collected.Count);
            Assert.Equal(Enumerable.Range(0, count), collected);
        }
    }
}