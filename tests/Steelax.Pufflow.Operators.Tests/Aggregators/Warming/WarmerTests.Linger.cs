using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmerTests
{
    public sealed class Linger
    {
        [Fact]
        public void PartialSegment_SealedAfterLingerFires()
        {
            var time = new ManualTimeProvider();
            using var warmer = Create(timeProvider: time, segmentCapacity: 5);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10), (2, 20));
            Assert.False(warmer.WarmNext(sink, out _, out _));

            time.Timer.Fire();

            Assert.True(warmer.WarmNext(sink, out var keys, out var watermark));
            Assert.Equal(new[] { 1, 2 }, keys);
            Assert.Equal(Watermark.From(20), watermark);
        }

        [Fact]
        public void TimerReArmed_WhenTailCannotBeSealed()
        {
            var time = new ManualTimeProvider();
            var factory = new TcsJobFactory();
            using var warmer = Create(factory, timeProvider: time, maxConcurrency: 1, maxQueued: 2, segmentCapacity: 2);
            var sink = new WarmSink();

            // A is full and occupies the only slot; B is a partial tail.
            AddKeys(warmer, (1, 10), (2, 20));
            AddKeys(warmer, (3, 30));

            var changeCallsBefore = time.Timer.ChangeCalls.Count;

            // Linger fired but the slot is busy → the tail is not sealed and the timer is re-armed.
            time.Timer.Fire();
            Assert.False(warmer.WarmNext(sink, out _, out _));

            Assert.True(time.Timer.ChangeCalls.Count > changeCallsBefore);

            // The slot frees up; a repeated linger seals the partial tail.
            factory.Created[0].Tcs.SetResult();
            time.Timer.Fire();

            // Pump WarmNext until B is sealed (the second job is created).
            Assert.True(SpinWait.SpinUntil(() =>
            {
                warmer.WarmNext(sink, out _, out _); // the head A is emitted along the way
                return factory.Created.Count == 2;
            }, 2000), "Expected the tail to be sealed after the slot was freed.");

            factory.Created[1].Tcs.SetResult();
            Assert.True(warmer.WarmNext(sink, out var keys, out _)); // B
            Assert.Equal(new[] { 3 }, keys);
        }

        [Fact]
        public void RealTimer_SealsPartialSegmentAfterLinger()
        {
            var sink = new WarmSink();
            using var warmer = new Warmer<int, string>(
                2,
                4,
                5,
                TimeSpan.FromMilliseconds(200),
                new SyncJobFactory(),
                TimeProvider.System);

            warmer.AddKey(1, Watermark.From(10));
            warmer.AddKey(2, Watermark.From(20));

            // Until the real timer fires, the partial segment is not emitted.
            Assert.False(warmer.WarmNext(sink, out _, out _));

            // The real timer fires after ~200 ms — wait for the seal and extraction.
            Assert.True(SpinWait.SpinUntil(() => warmer.WarmNext(sink, out _, out _), 5000),
                "Linger did not seal the partial segment.");

            Assert.Equal(new[] { (1, "W1"), (2, "W2") }, sink.Items);
        }
    }
}