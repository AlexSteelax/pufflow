namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmerTests
{
    public sealed class Warming
    {
        [Fact]
        public void Fill_StartsJobSynchronouslyAndSignals()
        {
            var callback = new RecordingCallback();
            using var warmer = Create(onReady: callback.Invoke, segmentCapacity: 2);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10), (2, 20));

            // Сегмент заполнен — задача запущена синхронно и подняла сигнал готовности.
            Assert.True(callback.Count > 0);

            Assert.True(warmer.WarmNext(sink, out var keys, out var watermark));

            Assert.Equal(new[] { 1, 2 }, keys);
            Assert.Equal(Watermark.From(20), watermark);
            Assert.Equal(new[] { (1, "W1"), (2, "W2") }, sink.Items);
        }

        [Fact]
        public void PartialSegment_NoJobStarted_UntilLinger()
        {
            var factory = new SyncJobFactory();
            var time = new ManualTimeProvider();
            using var warmer = Create(jobFactory: factory, timeProvider: time, segmentCapacity: 5);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10), (2, 20));

            // Частичный сегмент не запускается до linger.
            Assert.Equal(0, factory.CreatedCount);
            Assert.False(warmer.WarmNext(sink, out var keys, out var watermark));
            Assert.Null(keys);
            Assert.True(watermark.IsNothing);
            Assert.Empty(sink.Items);

            // Срабатывание linger запечатывает и запускает частичный сегмент.
            time.Timer.Fire();

            Assert.True(warmer.WarmNext(sink, out keys, out watermark));
            Assert.Equal(new[] { 1, 2 }, keys);
            Assert.Equal(Watermark.From(20), watermark);
            Assert.Equal(new[] { (1, "W1"), (2, "W2") }, sink.Items);
        }

        [Fact]
        public void NoCompletedHead_ReturnsFalse()
        {
            using var warmer = Create(segmentCapacity: 5);
            var sink = new WarmSink();

            AddKeys(warmer, (1, 10));

            Assert.False(warmer.WarmNext(sink, out var keys, out var watermark));
            Assert.Null(keys);
            Assert.True(watermark.IsNothing);
            Assert.Empty(sink.Items);
        }

        [Fact]
        public void Empty_ReturnsFalse()
        {
            using var warmer = Create();
            var sink = new WarmSink();

            Assert.False(warmer.WarmNext(sink, out var keys, out var watermark));
            Assert.Null(keys);
            Assert.True(watermark.IsNothing);
            Assert.Empty(sink.Items);
        }
    }
}