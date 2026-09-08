using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public sealed class Ordering
    {
        [Fact(Timeout = 1_000)]
        public async Task EmitsHeadOfLine_DespiteOutOfOrderCompletion()
        {
            var factory = new WarmingHelper.TcsJobFactory();
            await using var warmer = Create(factory, maxConcurrency: 3, maxQueued: 3, segmentCapacity: 1);
            var sink = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2, 3]);
            Assert.Equal(3, factory.Source.Count);

            // Complete C and B before A — the output is still strictly in order.
            factory.Source[2].SetResult();
            factory.Source[1].SetResult();

            Assert.False(warmer.WarmNext(sink, WarmMode.Normal, out _, out _)); // head A has not completed yet

            factory.Source[0].SetResult();

            // The jobs complete on async continuations; drain whatever is ready and wait for all three.
            var collected = new List<int[]>();
            Assert.WaitUntil(() =>
            {
                while (warmer.WarmNext(sink, WarmMode.Normal, out var keys, out _))
                    collected.Add(keys);
                return collected.Count == 3;
            }, TestContext.Current.CancellationToken, "Expected all three segments to be emitted in order.");

            Assert.Collection(collected,
                k => Assert.Equal(new[] { 1 }, k),
                k => Assert.Equal(new[] { 2 }, k),
                k => Assert.Equal(new[] { 3 }, k));
            Assert.Equal(3, sink.Items.Count);
            Assert.Equal("W1", sink.Items[1].Result);
            Assert.Equal("W2", sink.Items[2].Result);
            Assert.Equal("W3", sink.Items[3].Result);
        }

        [Fact]
        public async Task Watermarks_AllReal()
        {
            await using var warmer = Create(maxConcurrency: 2, maxQueued: 4, segmentCapacity: 2);
            var policy = new WarmingHelper.DefaultPolicy();

            AddKeys(warmer, [1, 2, 3, 4]);

            var watermarks = new List<Watermark>();
            while (warmer.WarmNext(policy, WarmMode.Normal, out _, out var watermark))
                watermarks.Add(watermark);

            // Two segments were emitted: both carry a real (non-Nothing) covering watermark.
            Assert.Equal(2, watermarks.Count);
            Assert.All(watermarks, w => Assert.False(w.IsNothing));
        }

        [Fact(Timeout = 1_000)]
        public async Task LargeScale_OutOfOrderCompletion_PreservesHeadOfLine()
        {
            const int segments = 10;
            var factory = new WarmingHelper.TcsJobFactory();
            await using var warmer = Create(factory, maxConcurrency: segments, maxQueued: segments,
                segmentCapacity: 1);
            var sink = new WarmingHelper.DefaultPolicy();

            for (var i = 1; i <= segments; i++)
                warmer.AddKey(i, Watermark.From(i * 10L));

            Assert.Equal(segments, factory.Source.Count);

            // Complete all jobs in reverse order — the output is still strictly in order.
            for (var i = segments - 1; i >= 0; i--)
                factory.Source[i].SetResult();

            // The jobs complete on async continuations; drain whatever is ready and wait for all segments.
            var collected = new List<int[]>();
            Assert.WaitUntil(() =>
            {
                while (warmer.WarmNext(sink, WarmMode.Normal, out var keys, out _))
                    collected.Add(keys);
                return collected.Count == segments;
            }, TestContext.Current.CancellationToken, "Expected all segments to be emitted in head-of-line order.");

            Assert.Equal(segments, collected.Count);
            for (var i = 0; i < segments; i++)
                Assert.Equal(new[] { i + 1 }, collected[i]);
        }
    }
}
