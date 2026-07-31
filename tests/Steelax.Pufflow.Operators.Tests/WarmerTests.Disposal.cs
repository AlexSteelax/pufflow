namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmerTests
{
    public sealed class Disposal
    {
        [Fact]
        public void Dispose_CancelsAndDisposesRunningJobs()
        {
            var factory = new TcsJobFactory();
            var warmer = Create(jobFactory: factory, maxConcurrency: 1, maxQueued: 2, segmentCapacity: 1);

            AddKeys(warmer, (1, 10));
            var job = factory.Created[0];
            Assert.False(job.CancellationToken.IsCancellationRequested);

            warmer.Dispose();

            Assert.True(job.CancellationToken.IsCancellationRequested);
            Assert.True(job.Disposed);
        }

        [Fact]
        public async Task DisposeAsync_DoesNotThrow()
        {
            var factory = new TcsJobFactory();
            var warmer = Create(jobFactory: factory, maxConcurrency: 1, maxQueued: 2, segmentCapacity: 1);

            AddKeys(warmer, (1, 10));

            await warmer.DisposeAsync();
        }
    }
}