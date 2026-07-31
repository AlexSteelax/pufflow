using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmerTests
{
    public sealed class Scale
    {
        [Fact]
        public void ManySegments_AllEmittedInOrder()
        {
            using var warmer = Create(maxConcurrency: 2, maxQueued: 10, segmentCapacity: 3);
            var sink = new WarmSink();

            for (var i = 1; i <= 30; i++)
                warmer.AddKey(i, Watermark.From(i * 10L));

            var collected = new List<int[]>();
            var watermarks = new List<long>();
            while (warmer.WarmNext(sink, out var keys, out var watermark))
            {
                collected.Add(keys!);
                watermarks.Add(watermark);
            }

            Assert.Equal(10, collected.Count);
            Assert.Equal(30, sink.Items.Count);

            for (var s = 0; s < 10; s++)
            {
                Assert.Equal(new[] { s * 3 + 1, s * 3 + 2, s * 3 + 3 }, collected[s]);
                Assert.Equal((s + 1) * 30L, watermarks[s]);
            }

            // Head-of-line: ключи в строгом порядке поступления.
            Assert.Equal(Enumerable.Range(1, 30).Select(i => (i, "W" + i)), sink.Items);
        }
    }

    /// <summary>
    /// Прогоняет большой объём данных через <see cref="Warmer{TKey,TWarm}"/> и проверяет,
    /// что каждое значение выдано ровно один раз и в строгом порядке поступления —
    /// по образцу <c>EventQueueTests.Concurrency.ConcurrentProducerConsumer_InputMatchesOutput</c>.
    /// </summary>
    public sealed class LargeVolume(ITestOutputHelper output)
    {
        [Theory(Timeout = 10000)]
        [InlineData(500_000, 1, 1, 1)]
        [InlineData(500_000, 4, 16, 4)]
        [InlineData(250_000, 8, 64, 8)]
        [InlineData(100_000, 32, 256, 32)]
        [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
        public async Task AddAndDrain_NoLossNoDuplicates(int count, int maxConcurrency, int maxQueued, int segmentCapacity)
        {
            var watch = Stopwatch.StartNew();
            await using var warmer = Create(maxConcurrency: maxConcurrency, maxQueued: maxQueued, segmentCapacity: segmentCapacity);
            var sink = new WarmSink();
            using var ready = new ManualResetEventSlim();

            warmer.OnReady += ready.Set;

            var worker = Task.Factory.StartNew(() =>
            {
                var i = 0;

                while (i < count)
                {
                    // Дреним готовые сегменты (освобождаем ring под backpressure).
                    while (warmer.WarmNext(sink, out _, out _)) { }

                    if (warmer.CanAdd)
                    {
                        warmer.AddKey(i, Watermark.From((long)i * 10));
                        i++;
                        continue;
                    }

                    // Места нет — ждём завершения в-flight задачи (head-of-line головки).
                    ready.Reset();
                    if (!warmer.WarmNext(sink, out _, out _) && !warmer.CanAdd)
                        ready.Wait();
                }

                // Источник исчерпан — запечатываем хвостовой сегмент и доедаем всё.
                warmer.Flush();

                while (!warmer.IsEmpty)
                {
                    while (warmer.WarmNext(sink, out _, out _)) { }

                    if (!warmer.IsEmpty)
                    {
                        ready.Reset();
                        if (!warmer.WarmNext(sink, out _, out _) && !warmer.IsEmpty)
                            ready.Wait();
                    }
                }
            }, TaskCreationOptions.LongRunning);

            await worker.WaitAsync(TimeSpan.FromSeconds(120), TestContext.Current.CancellationToken);

            watch.Stop();

            Assert.Equal(count, sink.Items.Count);
            Assert.Equal(Enumerable.Range(0, count).Select(i => (i, "W" + i)), sink.Items);

            output.WriteLine(watch.ElapsedMilliseconds is var elapsed && elapsed != 0 ? $"Time elapsed: {1m * count / elapsed:F3} item/ms" : "Time elapsed: - item/ms");
        }

        [Fact(Timeout = 15000)]
        [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
        public async Task DelayedJobs_AllEmittedExactlyOnce()
        {
            const int count = 10_000;
            const int delayMs = 2;

            var watch = Stopwatch.StartNew();
            await using var warmer = Create(jobFactory: new DelayedJobFactory(delayMs), maxConcurrency: 8, maxQueued: 32, segmentCapacity: 32);
            var sink = new WarmSink();

            var worker = Task.Factory.StartNew(() =>
            {
                var i = 0;
                var spin = new SpinWait();
                var spinCount = 0L;

                while (i < count)
                {
                    // Голову выгружаем всегда: готовые сегменты за головой могут не сигналить
                    // (edge-triggered OnReady), если очередь забита готовыми задачами.
                    while (warmer.WarmNext(sink, out _, out _)) { }

                    if (warmer.CanAdd)
                    {
                        warmer.AddKey(i, Watermark.From((long)i * 10));
                        i++;
                        spin.Reset();
                        spinCount = 0;
                        continue;
                    }

                    // Места нет — крутимся, пока не освободится место (джобы завершаются параллельно).
                    spin.SpinOnce();

                    if (++spinCount % 1_000_000 == 0)
                        output.WriteLine($"SPIN-STUCK: i={i}, canAdd={warmer.CanAdd}, queueFilled={warmer.QueueFilled}, isEmpty={warmer.IsEmpty}");
                }

                // Источник исчерпан — запечатываем хвостовой сегмент и доедаем всё.
                warmer.Flush();

                while (!warmer.IsEmpty)
                {
                    while (warmer.WarmNext(sink, out _, out _)) { }

                    if (!warmer.IsEmpty)
                        spin.SpinOnce();
                }
            }, TaskCreationOptions.LongRunning);

            await worker.WaitAsync(TimeSpan.FromSeconds(120), TestContext.Current.CancellationToken);

            watch.Stop();

            Assert.Equal(count, sink.Items.Count);
            Assert.Equal(Enumerable.Range(0, count).Select(i => (i, "W" + i)), sink.Items);

            output.WriteLine(watch.ElapsedMilliseconds is var elapsed && elapsed != 0 ? $"Time elapsed: {1m * count / elapsed:F3} item/ms" : "Time elapsed: - item/ms");
        }
    }
}