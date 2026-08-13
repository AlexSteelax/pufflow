using System.Threading.Channels;
using Steelax.Pufflow;
using Steelax.Pufflow.Operators;
using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Chunking;

/// <summary>
///     Black-box tests for the <c>Chunking(minimumSize, linger)</c> operator (backed by
///     <see cref="ChunkProcessor{T,TChunk}" />): an async consumator source feeds elements through the
///     chunker and the resulting <see cref="Chunk{T}" /> stream is observed downstream.
/// </summary>
public static partial class ChunkProcessorTests
{
    private const int TimeoutMs = 2_000;

    // --- helpers ---

    /// <summary>Runs the <c>Chunking</c> pipeline over an eagerly filled source and collects the chunk arrays.</summary>
    private static async Task<List<int[]>> RunAsync(IEnumerable<int> input, int size, TimeSpan linger, FlowSource flow)
    {
        flow
            .OnAsyncConsumatorSource(input)
            .Chunking(size, linger)
            .Consume(out var reader);

        await flow.ExecuteAsync();

        return await ReadChunksAsync(reader);
    }

    /// <summary>Runs the <c>Chunking</c> pipeline over a writer the caller fills with timing, then collects chunks.</summary>
    private static async Task<List<int[]>> RunTimedAsync(
        FlowSource flow,
        int size,
        TimeSpan linger,
        Func<ChannelWriter<int>, Task> fillAsync)
    {
        flow
            .OnAsyncConsumatorSource(out ChannelWriter<int> writer)
            .Chunking(size, linger)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync();

        await fillAsync(writer);
        writer.TryComplete();

        await runTask;

        return await ReadChunksAsync(reader);
    }

    private static async Task<List<int[]>> ReadChunksAsync(ChannelReader<Chunk<int>> reader)
    {
        var result = new List<int[]>();

        await foreach (var chunk in reader.ReadAllAsync(TestContext.Current.CancellationToken))
        {
            using (chunk)
                result.Add(chunk.Span.ToArray());
        }

        return result;
    }

    /// <summary>Fills the writer with <paramref name="values" />, delaying <paramref name="gapMs" /> between consecutive items.</summary>
    private static async Task FillDelayedAsync(ChannelWriter<int> writer, int gapMs, params int[] values)
    {
        for (var i = 0; i < values.Length; i++)
        {
            writer.TryWrite(values[i]);

            if (i < values.Length - 1)
                await Task.Delay(gapMs);
        }
    }

    /// <summary>
    ///     Fills each segment's values back-to-back, then waits <paramref name="segments" />' gap before
    ///     advancing to the next segment.
    /// </summary>
    private static async Task FillSegmentedAsync(ChannelWriter<int> writer, params (int[] Values, int GapMs)[] segments)
    {
        foreach (var (values, gapMs) in segments)
        {
            foreach (var value in values)
                writer.TryWrite(value);

            if (gapMs > 0)
                await Task.Delay(gapMs);
        }
    }

    private static void AssertChunks(List<int[]> actual, params int[][] expected)
    {
        Assert.Equal(expected.Length, actual.Count);
        for (var i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], actual[i]);
    }
}
