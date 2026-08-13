namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmProcessorTests
{
    public sealed class Scale
    {
        [Fact(Timeout = 20_000)]
        public async Task LargeInput_WithDelayedWarmJobs_AllDelivered()
        {
            // Большой поток: каждый ключ встречается один раз, чётные греются (джоб с задержкой),
            // нечётные идут passthrough. Проверяем отсутствие потерь и корректность выдачи.
            const int n = 1_000;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i, Watermark.From(i)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // греем чётные

            var results = await RunAsync(
                CreateWarmer(new DelayedJobFactory(delayMs: 15)),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();

            // Нечётные — passthrough (в порядке источника), чётные — группы (в порядке сегментов).
            Assert.Equal(
                Enumerable.Range(0, n).Where(static i => i % 2 != 0),
                values);
            Assert.Equal(
                Enumerable.Range(0, n).Where(static i => i % 2 == 0).Select(static i => i.ToString()),
                groups);

            Assert.Contains(results, static r => r.IsT2);
            Assert.Equal(n / 2, policy.Warmed.Count);
        }

        [Fact(Timeout = 20_000)]
        public async Task LargeInput_SyncJobs_SameCountOnOutput()
        {
            // Длинная дистанция: на выходе ровно столько же полезных записей (T0 passthrough +
            // T1 группы), сколько было подано на вход — ни одна запись не теряется и не дублируется.
            const int n = 1_000_000;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i, Watermark.From(i)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // греем чётные, нечётные — passthrough

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();

            // Каждый чётный ключ дал группу, каждый нечётный — passthrough; ничего не потеряно.
            Assert.Equal(n / 2, values.Length);
            Assert.Equal(n / 2, groups.Length);
            Assert.Equal(n, values.Length + groups.Length);

            // Порядок passthrough сохраняется.
            Assert.Equal(Enumerable.Range(0, n).Where(static i => i % 2 != 0), values);

            // Группы вышли в порядке сегментов.
            Assert.Equal(
                Enumerable.Range(0, n).Where(static i => i % 2 == 0).Select(static i => i.ToString()),
                groups);

            Assert.Contains(results, static r => r.IsT2);
            Assert.Equal(n / 2, policy.Warmed.Count);
        }
    }
}
