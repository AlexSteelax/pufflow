namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmProcessorTests
{
    public sealed class Watchdog
    {
        [Fact(Timeout = 60_000)]
        public async Task Enabled_ShortPeriod_LongPipelineCompletes()
        {
            // Watchdog включён с малым периодом: частые паразитные пробуждения не должны
            // ломать корректность — все записи доставлены, поток завершается.
            const int n = 500;
            var input = Enumerable.Range(0, n)
                .Select(i => new Watermarked<int>(i, Watermark.From(i)))
                .ToArray();

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // греем чётные

            var results = await RunAsync(
                CreateWarmer(new DelayedJobFactory(delayMs: 2)),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context,
                watchdogPeriod: TimeSpan.FromMilliseconds(10));

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();

            Assert.Equal(n / 2, values.Length);
            Assert.Equal(n / 2, groups.Length);
            Assert.Equal(n, values.Length + groups.Length);
            Assert.Contains(results, static r => r.IsT2);
            Assert.Equal(n / 2, policy.Warmed.Count);
        }

        [Fact(Timeout = 10_000)]
        public async Task DisabledByDefault_CompletesImmediately()
        {
            // watchdogPeriod не передан → null → watchdog выключен (как и раньше).
            await using var flow = new FlowSource();

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                new TestPolicy(),
                new ListAccumulatorFactory(),
                [],
                flow.Context);

            Assert.Empty(results);
        }

        [Fact(Timeout = 10_000)]
        public async Task InfinitePeriod_Disabled_CompletesImmediately()
        {
            // Явный Timeout.InfiniteTimeSpan → watchdog выключен.
            await using var flow = new FlowSource();

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                new TestPolicy(),
                new ListAccumulatorFactory(),
                [],
                flow.Context,
                watchdogPeriod: Timeout.InfiniteTimeSpan);

            Assert.Empty(results);
        }
    }
}
