namespace Steelax.Pufflow.Operators.Tests;

public static partial class WarmProcessorTests
{
    public sealed class Passthrough
    {
        [Fact(Timeout = 10_000)]
        public async Task NonWarmableValues_PassThroughInOrder_ThenProgressWatermark()
        {
            var input = new List<Watermarked<int>>
            {
                new(1, Watermark.From(10)),
                new(3, Watermark.From(30)),
                new(5, Watermark.From(50))
            };

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // греем чётные — нечётные идут passthrough

            var results = await RunAsync(
                CreateWarmer(new SyncJobFactory()),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow.Context);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            Assert.Equal(new[] { 1, 3, 5 }, values);

            // Глобальный progress-водяной знак (максимум из входных) выходит в конце как T2.
            var watermarks = results.Where(static r => r.IsT2).Select(static r => r.AsT2).ToArray();
            Assert.Equal(new[] { Watermark.From(50) }, watermarks);
            Assert.True(results[^1].IsT2, "watermark должен быть последним");
        }
    }
}