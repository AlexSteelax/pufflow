namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
/// Unit tests for the <see cref="BufferProcessor{T, TBuffer}"/> class and the
/// <see cref="BufferedChannel{T}"/> buffer.
/// </summary>
public static partial class BufferProcessorTests
{
    // --- helpers ---

    private static async Task<List<int>> CollectAsync(
        BufferProcessor<int> processor,
        IAsyncEnumerable<int> source)
    {
        await using var sourceEnumerator = source.GetAsyncEnumerator(TestContext.Current.CancellationToken);
        await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

        var result = new List<int>();
        while (await enumerator.MoveNextAsync())
            result.Add(enumerator.Current);

        return result;
    }

    private static async IAsyncEnumerable<int> FaultySourceAsync(Exception ex)
    {
        yield return 1;
        throw ex;
    }
}
