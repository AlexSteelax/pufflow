namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    public sealed class Disposal
    {
        [Fact(Timeout = 1_000)]
        public async Task DisposeAsync_CancelsRunningJob_AndCompletes()
        {
            var factory = new WarmingHelper.TcsJobFactory();
            var warmer = Create(factory, maxConcurrency: 1, maxQueued: 2, segmentCapacity: 1);

            AddKeys(warmer, [1]);

            // The job is suspended on an unresolved TCS; DisposeAsync must cancel it and not hang.
            Assert.False(factory.Source[0].Task.IsCompleted);

            await warmer.DisposeAsync().AsTask().WaitAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 1_000)]
        public async Task DisposeAsync_DoesNotThrow()
        {
            var factory = new WarmingHelper.TcsJobFactory();
            var warmer = Create(factory, maxConcurrency: 1, maxQueued: 2, segmentCapacity: 1);

            AddKeys(warmer, [1]);

            await warmer.DisposeAsync().AsTask().WaitAsync(TestContext.Current.CancellationToken);
        }
    }
}