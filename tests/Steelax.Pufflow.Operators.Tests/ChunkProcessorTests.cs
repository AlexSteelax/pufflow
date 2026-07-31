namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
/// Unit tests for the <see cref="ChunkProcessor{T}"/> class.
/// </summary>
public static partial class ChunkProcessorTests
{
    // --- helpers ---

    private static async Task<List<int[]>> CollectAsync(ChunkProcessor<int> processor, IAsyncEnumerable<int> source)
    {
        await using var sourceEnumerator = source.GetAsyncEnumerator(TestContext.Current.CancellationToken);
        await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

        var result = new List<int[]>();
        while (await enumerator.MoveNextAsync())
        {
            var chunk = enumerator.Current;
            result.Add(chunk.Span.ToArray());
            chunk.Dispose();
        }

        return result;
    }

    private static void AssertChunks(List<int[]> actual, params int[][] expected)
    {
        Assert.Equal(expected.Length, actual.Count);
        for (var i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], actual[i]);
    }

    private static async IAsyncEnumerable<int> DelayedSourceAsync(int gapMs, params int[] values)
    {
        for (var i = 0; i < values.Length; i++)
        {
            yield return values[i];
            if (i < values.Length - 1)
                await Task.Delay(gapMs);
        }
    }

    /// <summary>
    /// Yields each segment's values back-to-back, then waits <paramref name="segments"/>' gap
    /// before advancing to the next segment.
    /// </summary>
    private static async IAsyncEnumerable<int> SegmentedSourceAsync(params (int[] Values, int GapMs)[] segments)
    {
        foreach (var (values, gapMs) in segments)
        {
            foreach (var value in values)
                yield return value;

            if (gapMs > 0)
                await Task.Delay(gapMs);
        }
    }

    private static async IAsyncEnumerable<int> FaultySourceAsync(Exception ex)
    {
        yield return 1;
        throw ex;
    }
}
