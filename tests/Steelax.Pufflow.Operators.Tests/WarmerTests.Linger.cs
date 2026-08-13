namespace Steelax.Pufflow.Operators.Tests;

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

            // A заполнен и занимает единственный слот; B — частичный tail.
            AddKeys(warmer, (1, 10), (2, 20));
            AddKeys(warmer, (3, 30));

            var changeCallsBefore = time.Timer.ChangeCalls.Count;

            // Linger сработал, но слот занят → tail не запечатан, таймер взводится заново.
            time.Timer.Fire();
            Assert.False(warmer.WarmNext(sink, out _, out _));

            Assert.True(time.Timer.ChangeCalls.Count > changeCallsBefore);

            // Слот освобождается; повторный linger запечатывает частичный tail.
            factory.Created[0].Tcs.SetResult();
            time.Timer.Fire();

            // Крутим WarmNext, пока B не будет запечатан (создана вторая задача).
            Assert.True(SpinWait.SpinUntil(() =>
            {
                warmer.WarmNext(sink, out _, out _); // попутно эмитится head A
                return factory.Created.Count == 2;
            }, 2000), "Ожидалось запечатывание tail после освобождения слота.");

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

            // До срабатывания реального таймера частичный сегмент не выдаётся.
            Assert.False(warmer.WarmNext(sink, out _, out _));

            // Реальный таймер срабатывает ~через 200 мс — ждём запечатывания и извлечения.
            Assert.True(SpinWait.SpinUntil(() => warmer.WarmNext(sink, out _, out _), 5000),
                "Linger не запечатал частичный сегмент.");

            Assert.Equal(new[] { (1, "W1"), (2, "W2") }, sink.Items);
        }
    }
}