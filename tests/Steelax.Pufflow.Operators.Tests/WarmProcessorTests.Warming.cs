namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmProcessorTests
{
    public sealed class Warming
    {
        [Fact(Timeout = 10_000)]
        public async Task WarmableValues_ProduceGroups_AndWatermark()
        {
            var input = new List<Watermarked<int>>
            {
                new(2, Watermark.From(20)),
                new(4, Watermark.From(40)),
            };

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // греем чётные (2, 4)

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context);

            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();
            Assert.Equal(new[] { "2", "4" }, groups);

            Assert.Contains(results, static r => r.IsT2);
            Assert.Equal(2, policy.Warmed.Count);
        }

        [Fact(Timeout = 10_000)]
        public async Task Mixed_PassthroughAndWarmable_AllEmitted()
        {
            var input = new List<Watermarked<int>>
            {
                new(1, Watermark.From(10)), // passthrough
                new(2, Watermark.From(20)), // warm
                new(3, Watermark.From(30)), // passthrough
                new(4, Watermark.From(40)), // warm
            };

            await using var flow = new FlowSource();
            var policy = new TestPolicy();

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            var groups = results.Where(static r => r.IsT1).Select(static r => r.AsT1).ToArray();

            Assert.Equal(new[] { 1, 3 }, values);
            Assert.Equal(new[] { "2", "4" }, groups);
        }

        [Fact(Timeout = 10_000)]
        public async Task EmptySource_CompletesImmediately()
        {
            await using var flow = new FlowSource();

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                new TestPolicy(),
                new ListAccumulatorFactory(),
                [],
                flow.Context);

            Assert.Empty(results);
        }
    }
}
