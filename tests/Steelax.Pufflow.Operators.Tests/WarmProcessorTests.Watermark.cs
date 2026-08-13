namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmProcessorTests
{
    public sealed class WatermarkSequence
    {
        [Fact(Timeout = 60_000)]
        public async Task MonotonicWatermarks_OneKey_LargeInput_LastWatermarkEmitted()
        {
            // Один и тот же warmable ключ на 500 позиций, вотермарка растёт с каждым сообщением
            // (10 → 20 → … → 5000). На выходе вотермарка — именно последняя (максимальная).
            const int n = 500;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(2, Watermark.From((i + 1) * 10)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // греем чётные (2)

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context);

            // Ключ warmable — passthrough быть не должно.
            Assert.DoesNotContain(results, static r => r.IsT0);

            // Все значения ключа аккумулированы в группу (одна группа на ключ).
            Assert.Equal(1, results.Count(static r => r.IsT1));

            // Ни одна вотермарка на выходе не превышает последнюю (максимальную) из переданных.
            var watermarks = results.Where(static r => r.IsT2).Select(static r => r.AsT2).ToArray();
            Assert.NotEmpty(watermarks);
            Assert.All(watermarks, w => Assert.True(w <= Watermark.From(n * 10)));

            // Финальная (глобальная progress) вотермарка — ровно последняя из входных.
            Assert.True(results[^1].IsT2, "watermark должен быть последним");
            Assert.Equal(Watermark.From(n * 10), results[^1].AsT2);
        }

        [Fact(Timeout = 60_000)]
        public async Task MonotonicWatermarks_UniqueKeys_LargeInput_LastWatermarkEmitted()
        {
            // 500 уникальных warmable ключей, вотермарка монотонно растёт. Финальная вотермарка
            // на выходе должна быть последней (максимальной), а не первой или промежуточной.
            const int n = 500;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i * 2, Watermark.From((i + 1) * 10)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // греем чётные

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context);

            Assert.DoesNotContain(results, static r => r.IsT0);

            // Каждая позиция — отдельная группа (в порядке сегментов).
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();
            Assert.Equal(n, groups.Length);

            var watermarks = results.Where(static r => r.IsT2).Select(static r => r.AsT2).ToArray();
            Assert.NotEmpty(watermarks);

            // Финальная вотермарка — максимальная на выходе и равна последней из входных.
            Assert.Equal(Watermark.From(n * 10), watermarks.Max());
            Assert.True(results[^1].IsT2, "watermark должен быть последним");
            Assert.Equal(Watermark.From(n * 10), results[^1].AsT2);
        }
    }
}