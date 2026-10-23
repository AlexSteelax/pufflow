using System.Threading.Channels;
using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

/// <summary>
///     Tests for the second chunker (<c>Chunking</c>) that wraps chunks in a <see cref="Carrier{ChunkOfT}" /> where
///     the surrounding watermark is the maximum of every item arriving in that window.
/// </summary>
public static partial class CarrierChunkProcessorTests
{
    private const int TimeoutMs = 2_000;

    // --- helpers ---

    private static async Task<List<Carrier<Chunk<int>>>> RunAsync(
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

        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Runs the <c>Chunking</c> pipeline over a writer the caller fills with timing, then collects windows.</summary>
    private static async Task<List<Carrier<Chunk<int>>>> RunTimedAsync(
        FlowSource flow,
        int size,
        TimeSpan linger,
        Func<ChannelWriter<Carrier<int>>, Task> fillAsync,
        CancellationToken cancellationToken)
    {
        flow
            .OnAsyncConsumatorSource(out ChannelWriter<Carrier<int>> writer)
            .Chunking(size, linger)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(cancellationToken);

        await fillAsync(writer);
        writer.TryComplete();

        await runTask;

        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }

    // --- assertions ---

    private static void AssertDataWindow(Carrier<Chunk<int>> window, int[] items, Watermark watermark)
    {
        Assert.True(window.HasValue, "expected a data chunk");
        Assert.Equal(items, window.Value.Span.ToArray());
        Assert.Equal(watermark, window.Watermark);
    }

    private static void AssertBareWindow(Carrier<Chunk<int>> window, Watermark watermark)
    {
        Assert.False(window.HasValue, "expected a bare progress carrier");
        Assert.Equal(watermark, window.Watermark);
    }
}