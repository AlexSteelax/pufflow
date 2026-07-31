using Unio;

namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
/// Unit tests for the <see cref="TimeoutProcessor{T}"/> class.
/// </summary>
public static partial class TimeoutProcessorTests
{
    // --- helpers ---

    private static async Task<List<Unio<int, AwaitTimeout>>> CollectAsync(
        TimeoutProcessor<int> processor,
        IAsyncEnumerable<int> source)
    {
        await using var sourceEnumerator = source.GetAsyncEnumerator(TestContext.Current.CancellationToken);
        await using var enumerator = processor.GetAsyncEnumerator(sourceEnumerator, default);

        var result = new List<Unio<int, AwaitTimeout>>();
        while (await enumerator.MoveNextAsync())
            result.Add(enumerator.Current);

        return result;
    }

    private static async IAsyncEnumerable<int> SyncBurstThenAsyncSourceAsync()
    {
        yield return 1; // synchronous
        yield return 2; // synchronous — second consecutive sync item triggers the fast path
        await Task.Delay(50); // async gap → the next MoveNext is pending
        yield return 3; // asynchronous
    }

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
