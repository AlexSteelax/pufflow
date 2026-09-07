using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Xunit;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

/// <summary>
///     Tests for the second chunker (<c>Chunking2</c>) that wraps chunks in a <see cref="Carrier{ChunkOfT}" /> where
///     the surrounding watermark is the maximum of every item arriving in that window.
/// </summary>
public class CarrierChunkProcessorTests
{
    private const int TimeoutMs = 2_000;

    private sealed record Window(int[] Items, Watermark Watermark, bool HasData);

    private static async Task<List<Window>> RunAsync(
        IReadOnlyList<Carrier<int>> input,
        int size,
        TimeSpan linger,
        CancellationToken cancellationToken)
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(input)
            .Chunking(size, linger)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        var result = new List<Window>();
        await foreach (var item in reader.ReadAllAsync(TestContext.Current.CancellationToken))
        {
            if (!item.HasValue)
            {
                // An empty carrier carries no chunk buffer, only a progress watermark.
                result.Add(new Window([], item.Watermark, HasData: false));
                continue;
            }

            using (var chunk = item.Value)
                result.Add(new Window(chunk.Span.ToArray(), item.Watermark, HasData: true));
        }

        return result;
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task DataWindow_CarriesChunkOfRawValues_AndMaxWatermark()
    {
        // Size == total data count → exactly one window flushing at end-of-stream. A couple of bare progress
        // items do not occupy the chunk buffer but must raise the window watermark to their maximum.
        var input = new Carrier<int>[]
        {
            new(1, Watermark.From(10)),
            new Carrier<int>(Watermark.From(30)), // bare progress
            new(2, Watermark.From(20)),
            new(3, Watermark.From(40))
        };

        var windows = await RunAsync(input, size: 3, linger: TimeSpan.FromMilliseconds(60_000), TestContext.Current.CancellationToken);

        Assert.NotNull(windows);
        var window = Assert.Single(windows);
        Assert.True(window.HasData);
        Assert.Equal(new[] { 1, 2, 3 }, window.Items);
        Assert.Equal(Watermark.From(40), window.Watermark);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task BareOnlyStream_Closes_WithEmptyCarrierProgressWatermark()
    {
        // A stream of only progress items has no data to chunk; at the end it must still emit a single empty
        // carrier so the (maximum) progress watermark moves forward.
        var input = new[]
        {
            new Carrier<int>(Watermark.From(50)),
            new Carrier<int>(Watermark.From(60)),
        };

        var windows = await RunAsync(input, size: 2, linger: TimeSpan.FromMilliseconds(60_000), TestContext.Current.CancellationToken);

        var window = Assert.Single(windows);
        Assert.False(window.HasData);
        Assert.Empty(window.Items);
        Assert.Equal(Watermark.From(60), window.Watermark);
    }
}
