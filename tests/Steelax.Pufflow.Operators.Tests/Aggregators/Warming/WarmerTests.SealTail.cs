using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Aggregators.Warming;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public sealed class SealTail
    {
        [Fact]
        public async Task PartialTail_NotSealedInNormal_ThenSealedOnSealTail()
        {
            var factory = new WarmingHelper.SyncJobFactory();
            await using var warmer = Create(factory, segmentCapacity: 5);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2]);

            // Normal mode leaves the partial tail open; SealTail starts it.
            Assert.False(warmer.WarmNext(sink, WarmMode.Normal, out _, out _));
            Assert.Equal(0, factory.CreatedCount);

            Assert.True(warmer.WarmNext(sink, WarmMode.SealTail, out var keys, out _));
            Assert.Equal(new[] { 1, 2 }, keys);
            Assert.Equal(1, factory.CreatedCount);
        }

        [Fact(Timeout = 1000)]
        public async Task PartialTail_NotSealedWhileBusy_ThenSealedWhenSlotFrees()
        {
            var factory = new WarmingHelper.TcsJobFactory();
            await using var warmer = Create(factory, maxConcurrency: 1, maxQueued: 2, segmentCapacity: 2);
            var sink = new WarmingHelper.DefaultPolicy();

            // A is full and occupies the only slot; B is a partial tail.
            AddKeys(warmer, [1, 2]); // A: full
            AddKeys(warmer, [3]); // B: partial

            // SealTail: the slot is occupied by A, so B cannot be started — nothing to emit yet.
            Assert.False(warmer.WarmNext(sink, WarmMode.SealTail, out _, out _));
            Assert.Single(factory.Source);

            // A completes, freeing the slot; a SealTail pass then starts the partial tail B.
            factory.Source[0].SetResult();
            
            Assert.WaitUntil(() =>
            {
                warmer.WarmNext(sink, WarmMode.SealTail, out _, out _); // the head A is emitted along the way
                return factory.Source.Count == 2;
            }, TestContext.Current.CancellationToken, "Expected the partial tail to be sealed after the slot was freed.");
            
            factory.Source[1].SetResult();

            int[]? keys = null;
            Assert.WaitUntil(() => warmer.WarmNext(sink, WarmMode.SealTail, out keys, out _), TestContext.Current.CancellationToken, "Expected the partial tail to be emitted once its job completed.");

            Assert.Equal(new[] { 3 }, keys);
        }
    }
}