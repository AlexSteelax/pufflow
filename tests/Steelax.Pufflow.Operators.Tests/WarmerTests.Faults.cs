namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmerTests
{
    public sealed class Faults
    {
        [Fact]
        public void FaultedHead_RethrowsOnExtraction()
        {
            using var warmer = Create(jobFactory: new FaultingJobFactory(), maxConcurrency: 1, maxQueued: 2, segmentCapacity: 1);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10));

            var ex = Assert.Throws<InvalidOperationException>(() => warmer.WarmNext(sink, out _, out _));
            Assert.Equal("boom", ex.Message);
        }

        [Fact]
        public void FaultedNonHead_DeferredUntilItBecomesHead()
        {
            var factory = new TcsJobFactory();
            using var warmer = Create(jobFactory: factory, maxConcurrency: 2, maxQueued: 2, segmentCapacity: 1);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10), (2, 20));

            // B падает раньше, чем завершается A — fault откладывается до head.
            factory.Created[1].Tcs.SetException(FaultingJob.Boom);

            Assert.False(warmer.WarmNext(sink, out _, out _)); // A ещё не завершён

            factory.Created[0].Tcs.SetResult();

            Assert.True(warmer.WarmNext(sink, out var keys, out _));
            Assert.Equal(new[] { 1 }, keys);

            var ex = Assert.Throws<InvalidOperationException>(() => warmer.WarmNext(sink, out _, out _));
            Assert.Equal("boom", ex.Message);
        }

        [Fact]
        public void CanceledHead_PropagatesTaskCanceled()
        {
            using var warmer = Create(jobFactory: new CanceledJobFactory(), maxConcurrency: 1, maxQueued: 2, segmentCapacity: 1);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10));

            Assert.ThrowsAny<OperationCanceledException>(() => warmer.WarmNext(sink, out _, out _));
        }
    }
}