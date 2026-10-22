using System.Threading.Channels;
using Steelax.Pufflow.Operators;
using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Unio;

namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
///     Shared helpers for operator tests: common pipeline runners, channel filling, and carrier/union
///     extraction utilities.
/// </summary>
public static class TestHelper
{
    /// <summary>
    ///     Collapses consecutive duplicate values in a range into a single occurrence each,
    ///     preserving order. Non-consecutive equal values are kept.
    /// </summary>
    /// <typeparam name="T">The element type; must support equality comparison.</typeparam>
    /// <param name="source">The input range.</param>
    /// <returns>A range with consecutive duplicates removed.</returns>
    public static T[] RangeDistinct<T>(IEnumerable<T> source)
        where T : IEquatable<T>
    {
        using var enumerator = source.GetEnumerator();

        if (!enumerator.MoveNext())
            return [];

        var result = new List<T> { enumerator.Current };
        var last = enumerator.Current;

        while (enumerator.MoveNext())
        {
            if (!enumerator.Current.Equals(last))
            {
                result.Add(enumerator.Current);
                last = enumerator.Current;
            }
        }

        return result.ToArray();
    }

    /// <summary>Fills the writer with <paramref name="values" />, delaying <paramref name="gapMs" /> between consecutive items.</summary>
    public static async Task FillDelayedAsync<T>(ChannelWriter<T> writer, int gapMs, params T[] values)
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
    public static async Task FillSegmentedAsync<T>(ChannelWriter<T> writer, params (T[] Values, int GapMs)[] segments)
    {
        foreach (var (values, gapMs) in segments)
        {
            foreach (var value in values)
                writer.TryWrite(value);

            if (gapMs > 0)
                await Task.Delay(gapMs);
        }
    }

    /// <summary>Runs an eagerly filled consumator source over <c>Chunking</c> and collects the chunk arrays.</summary>
    public static async Task<List<int[]>> RunChunkingAsync(
        IEnumerable<int> input,
        int size,
        TimeSpan linger,
        FlowSource flow,
        CancellationToken cancellationToken)
    {
        flow
            .OnAsyncConsumatorSource(input)
            .Chunking(size, linger)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        return await ReadChunksAsync(reader);
    }

    /// <summary>Runs a write-driven consumator source over <c>Chunking</c>, letting the caller fill with timing.</summary>
    public static async Task<List<int[]>> RunTimedChunkingAsync(
        FlowSource flow,
        int size,
        TimeSpan linger,
        Func<ChannelWriter<int>, Task> fillAsync,
        CancellationToken cancellationToken)
    {
        flow
            .OnAsyncConsumatorSource(out ChannelWriter<int> writer)
            .Chunking(size, linger)
            .Consume(out var reader);

        var runTask = flow.ExecuteAsync(cancellationToken);

        await fillAsync(writer);
        writer.TryComplete();

        await runTask;

        return await ReadChunksAsync(reader);
    }

    /// <summary>Drains a chunk reader into plain arrays of the chunk payloads.</summary>
    public static async Task<List<int[]>> ReadChunksAsync(ChannelReader<Chunk<int>> reader)
    {
        var result = new List<int[]>();

        await foreach (var chunk in reader.ReadAllAsync(TestContext.Current.CancellationToken))
        {
            using (chunk)
                result.Add(chunk.Span.ToArray());
        }

        return result;
    }

    /// <summary>Asserts chunk arrays have the same count and contents as the expected ones.</summary>
    public static void AssertChunks(List<int[]> actual, params int[][] expected)
    {
        Assert.Equal(expected.Length, actual.Count);
        for (var i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], actual[i]);
    }

    /// <summary>Pass-through values (carrier payload union T0) present in the output.</summary>
    public static TValue[] Values<TValue, TGroup>(IReadOnlyList<Carrier<Unio<TValue, TGroup>>> results)
    {
        return results.Where(static r => r is { HasValue: true, Value.IsT0: true }).Select(static r => r.Value.AsT0).ToArray();
    }

    /// <summary>Accumulated group results (carrier payload union T1) present in the output.</summary>
    public static TGroup[] Groups<TGroup>(IReadOnlyList<Carrier<Unio<int, TGroup>>> results)
    {
        return results.Where(static r => r is { HasValue: true, Value.IsT1: true }).Select(static r => r.Value.AsT1).ToArray();
    }

    /// <summary>The real progress watermarks carried by empty (bare) carriers.</summary>
    public static Watermark[] Progress<TValue, TGroup>(IReadOnlyList<Carrier<Unio<TValue, TGroup>>> results)
    {
        return results.Where(static w => w.HasWatermark).Select(static c => c.Watermark).ToArray();
    }

    /// <summary>Indicates whether the last output item is an empty (bare-progress) carrier.</summary>
    public static bool IsLastProgress<TValue, TGroup>(IReadOnlyList<Carrier<Unio<TValue, TGroup>>> results)
    {
        return results.Count > 0 && !results[^1].HasValue;
    }
}