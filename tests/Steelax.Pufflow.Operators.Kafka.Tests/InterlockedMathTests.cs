using Steelax.Pufflow.Operators.Kafka;
using Xunit;

namespace Steelax.Pufflow.Operators.Kafka.Tests;

/// <summary>
///     Concurrency tests for <see cref="InterlockedMath.AdvanceMax" />.
/// </summary>
public static class InterlockedMathTests
{
    [Fact]
    public static async Task ConcurrentAdvanceMax_IsMonotonicAndEndsAtMaximum()
    {
        var token = TestContext.Current.CancellationToken;
        const int count = 10_000;
        long value = 0;

        // Writer 1 races ahead with monotonically increasing values [1, count].
        var writer1 = Task.Run(() =>
        {
            for (var i = 1; i <= count; i++)
                InterlockedMath.AdvanceMax(ref value, i);
        }, token);

        // Writer 2 reads the writer 1's current progress and continues from there to the same ceiling,
        // so both writers converge on `count` and the winning order is irrelevant.
        var writer2 = Task.Run(() =>
        {
            var current = Volatile.Read(ref value);

            for (var i = current; i <= count - current; i++)
                InterlockedMath.AdvanceMax(ref value, i, current);
        }, token);

        // A reader verifies that the value never goes backwards between its own reads.
        var reader = Task.Run(() =>
        {
            var previous = Volatile.Read(ref value);

            while (!writer1.IsCompleted || !writer2.IsCompleted)
            {
                var current = Volatile.Read(ref value);

                if (current < previous)
                    throw new InvalidOperationException($"value went backwards: {previous} -> {current}");

                previous = current;
                Thread.Yield();
            }
        }, token);

        await Task.WhenAll(writer1, writer2, reader);
        
        Assert.True(writer1.IsCompletedSuccessfully);
        Assert.True(writer2.IsCompletedSuccessfully);
        Assert.True(reader.IsCompletedSuccessfully);

        // Both writers converge on the same ceiling, so the final value is exactly `count`.
        Assert.Equal(count, Volatile.Read(ref value));
    }
}
