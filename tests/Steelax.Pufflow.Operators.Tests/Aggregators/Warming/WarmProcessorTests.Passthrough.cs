using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Warming;

public static partial class WarmProcessorTests
{
    public sealed class Passthrough
    {
        [Fact(Timeout = 1_000)]
        public async Task NonWarmableValues_PassThroughInOrder_ThenProgressWatermark()
        {
            var input = new List<Watermarked<int>>
            {
                new(1, Watermark.From(10)),
                new(3, Watermark.From(30)),
                new(5, Watermark.From(50))
            };

            await using var flow = new FlowSource();
            var policy = new TestPolicy(); // warm even keys — odd ones pass through

            var results = await RunAsync(
                new SyncJobFactory(),
                policy,
                new ListAccumulatorFactory(),
                input,
                flow,
                null,
                TestContext.Current.CancellationToken);

            var values = results.Where(static r => r.IsT0).Select(static r => r.AsT0).ToArray();
            Assert.Equal(new[] { 1, 3, 5 }, values);

            // The global progress watermark (the maximum of the input) is emitted at the end as T2.
            var watermarks = results.Where(static r => r.IsT2).Select(static r => r.AsT2).ToArray();
            Assert.Equal(new[] { Watermark.From(50) }, watermarks);
            Assert.True(results[^1].IsT2, "watermark should be the last item");
        }
    }
}