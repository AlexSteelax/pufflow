using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Unio;

namespace Steelax.Pufflow.Operators.Tests.Aggregators;

/// <summary>
///     Black-box tests for the <c>Watermarked()</c> operator (backed by
///     <see cref="PairValueWatermarkProcessor{T}" />): a stream of <see cref="Unio{T, Watermark}" /> items
///     (values T0 interleaved with watermark markers T1) is pushed through the operator and the resulting
///     <see cref="Watermarked{T}" /> stream is observed. The operator holds one value (hold-slot) and attaches
///     the latest watermark to it when the next value or a watermark marker arrives (eviction).
/// </summary>
public static class PairValueWatermarkProcessorTests
{
    private const int TimeoutMs = 1_000;

    private static async Task<List<Watermarked<int>>> RunAsync(
        IEnumerable<Unio<int, Watermark>> input,
        FlowSource flow, CancellationToken cancellationToken)
    {
        flow
            .OnAsyncProducatorSource(input)
            .Watermarked()
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        return await reader.ReadAllAsync(cancellationToken).ToListAsync(cancellationToken);
    }

    public sealed class Collapse
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task Value_Then_Watermark_AttachesWatermarkToHeldValue()
        {
            // One value, then a watermark: the watermark attaches to the held value and the slot is released.
            var input = new List<Unio<int, Watermark>>
            {
                1,
                Watermark.From(10)
            };

            await using var flow = new FlowSource();
            var results = await RunAsync(input, flow, TestContext.Current.CancellationToken);

            var item = Assert.Single(results);
            Assert.Equal(1, item.Value);
            Assert.Equal(Watermark.From(10), item.Watermark);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task Value_Then_Value_AttachesPreviousWatermarkAndEvicts()
        {
            // Two consecutive values: the first is evicted by the second with a Nothing watermark (no
            // watermark has arrived yet), and the second is flushed at completion with the held watermark.
            var input = new List<Unio<int, Watermark>>
            {
                1,
                2
            };

            await using var flow = new FlowSource();
            var results = await RunAsync(input, flow, TestContext.Current.CancellationToken);

            Assert.Collection(results,
                item => Assert.Equal(new Watermarked<int>(1, Watermark.Nothing()), item),
                item => Assert.Equal(new Watermarked<int>(2, Watermark.Nothing()), item));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task Value_Watermark_Value_AttachesWatermarkToBoth()
        {
            // Value, watermark, value: the first gets watermark 10 (released by the watermark), the second
            // is flushed at completion with the current watermark — it was reset to Nothing when the first
            // value consumed the watermark 10.
            var input = new List<Unio<int, Watermark>>
            {
                1,
                Watermark.From(10),
                2
            };

            await using var flow = new FlowSource();
            var results = await RunAsync(input, flow, TestContext.Current.CancellationToken);

            Assert.Collection(results,
                item => Assert.Equal(new Watermarked<int>(1, Watermark.From(10)), item),
                item => Assert.Equal(new Watermarked<int>(2, Watermark.Nothing()), item));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task Value_Watermark_Value_Watermark_AllValuesCarryLatestWatermark()
        {
            // Value, watermark 10, value, watermark 20: the first goes out with 10, the second with 20.
            var input = new List<Unio<int, Watermark>>
            {
                Watermark.From(5),
                1,
                Watermark.From(10),
                2,
                Watermark.From(20)
            };

            await using var flow = new FlowSource();
            var results = await RunAsync(input, flow, TestContext.Current.CancellationToken);

            Assert.Collection(results,
                item => Assert.Equal(new Watermarked<int>(1, Watermark.From(10)), item),
                item => Assert.Equal(new Watermarked<int>(2, Watermark.From(20)), item));
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task EmptyInput_CompletesImmediately()
        {
            await using var flow = new FlowSource();
            var results = await RunAsync([], flow, TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task OnlyWatermarks_NoValues()
        {
            // Watermarks only, no values: nothing to emit, the slot stays empty.
            var input = new List<Unio<int, Watermark>>
            {
                Watermark.From(10),
                Watermark.From(20)
            };

            await using var flow = new FlowSource();
            var results = await RunAsync(input, flow, TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }
    }
}
