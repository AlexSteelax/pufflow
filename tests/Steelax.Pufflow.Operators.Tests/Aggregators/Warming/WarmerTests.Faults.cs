using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public sealed class Faults
    {
        [Fact]
        public async Task FaultedHead_RethrowsOnExtraction()
        {
            await using var warmer = Create(new WarmingHelper.FaultingJobFactory(), maxConcurrency: 1,
                maxQueued: 2, segmentCapacity: 1);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1]);

            var ex = Assert.Throws<InvalidOperationException>(() =>
                warmer.WarmNext(sink, WarmMode.Normal, out _, out _));
            Assert.Equal("boom", ex.Message);
        }

        [Fact(Timeout = 1_000)]
        public async Task FaultedNonHead_DeferredUntilItBecomesHead()
        {
            var factory = new WarmingHelper.TcsJobFactory();
            await using var warmer = Create(factory, maxConcurrency: 2, maxQueued: 2, segmentCapacity: 1);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2]);

            // B faults before A completes — the fault is deferred until it becomes the head.
            factory.Source[1].SetException(WarmingHelper.FaultingJobFactory.Boom);

            Assert.False(warmer.WarmNext(sink, WarmMode.Normal, out _, out _)); // A has not completed yet

            factory.Source[0].SetResult();

            int[]? keys = null;
            Assert.WaitUntil(() => warmer.WarmNext(sink, WarmMode.Normal, out keys, out _), TestContext.Current.CancellationToken, "Expected the head segment A to be extracted once its job completed.");

            Assert.Equal(new[] { 1 }, keys);

            // B is the head now; its fault surfaces once the job completes on the async continuation.
            var ex = default(InvalidOperationException);
            Assert.WaitUntil(() =>
            {
                try
                {
                    warmer.WarmNext(sink, WarmMode.Normal, out _, out _);
                    return false;
                }
                catch (InvalidOperationException caught)
                {
                    ex = caught;
                    return true;
                }
            }, TestContext.Current.CancellationToken, "Expected the fault of B to surface once it became the head.");

            Assert.Equal("boom", ex!.Message);
        }

        [Fact]
        public async Task CanceledHead_ThrowsOperationCanceled()
        {
            await using var warmer = Create(new WarmingHelper.CanceledJobFactory(), maxConcurrency: 1,
                maxQueued: 2, segmentCapacity: 1);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1]);

            Assert.ThrowsAny<OperationCanceledException>(() =>
                warmer.WarmNext(sink, WarmMode.Normal, out _, out _));
        }
    }
}