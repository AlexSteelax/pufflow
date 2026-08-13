using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    public sealed class Ordering
    {
        [Fact]
        public void EmitsHeadOfLine_DespiteOutOfOrderCompletion()
        {
            var factory = new TcsJobFactory();
            using var warmer = Create(factory, maxConcurrency: 3, maxQueued: 3, segmentCapacity: 1);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10), (2, 20), (3, 30));
            Assert.Equal(3, factory.Created.Count);

            // Complete C and B before A — the output is still strictly in order.
            factory.Created[2].Tcs.SetResult();
            factory.Created[1].Tcs.SetResult();

            Assert.False(warmer.WarmNext(sink, out _, out _)); // head A has not completed yet

            factory.Created[0].Tcs.SetResult();

            var collected = new List<int[]>();
            while (warmer.WarmNext(sink, out var keys, out _))
                collected.Add(keys!);

            Assert.Collection(collected,
                k => Assert.Equal(new[] { 1 }, k),
                k => Assert.Equal(new[] { 2 }, k),
                k => Assert.Equal(new[] { 3 }, k));
            Assert.Equal(new[] { (1, "W1"), (2, "W2"), (3, "W3") }, sink.Items);
        }

        [Fact]
        public void Watermarks_NonDecreasing()
        {
            using var warmer = Create(maxConcurrency: 2, maxQueued: 4, segmentCapacity: 2);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10), (2, 20), (3, 30), (4, 40));

            var watermarks = new List<long>();
            while (warmer.WarmNext(sink, out _, out var watermark))
                watermarks.Add(watermark);

            Assert.Equal(new[] { 20L, 40L }, watermarks);
        }

        [Fact]
        public void LargeScale_OutOfOrderCompletion_PreservesHeadOfLine()
        {
            const int segments = 10;
            var factory = new TcsJobFactory();
            using var warmer = Create(factory, maxConcurrency: segments, maxQueued: segments, segmentCapacity: 1);
            var sink = new WarmSink();

            for (var i = 1; i <= segments; i++)
                warmer.AddKey(i, Watermark.From(i * 10L));

            Assert.Equal(segments, factory.Created.Count);

            // Complete all jobs in reverse order — the output is still strictly in order.
            for (var i = segments - 1; i >= 0; i--)
                factory.Created[i].Tcs.SetResult();

            var collected = new List<int[]>();
            while (warmer.WarmNext(sink, out var keys, out _))
                collected.Add(keys!);

            Assert.Equal(segments, collected.Count);
            for (var i = 0; i < segments; i++)
                Assert.Equal(new[] { i + 1 }, collected[i]);
        }
    }
}