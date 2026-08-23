namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
///     Extended assertion helpers for operator tests.
/// </summary>
public static class AssertX
{
    /// <summary>
    ///     Asserts that after collapsing consecutive duplicates the range is strictly increasing.
    ///     Repeated values are allowed only as consecutive duplicates (e.g. the same watermark landing
    ///     in two adjacent windows); any non-increasing pair fails.
    /// </summary>
    /// <typeparam name="T">The element type; must be comparable with itself.</typeparam>
    /// <param name="source">The input range (order-sensitive).</param>
    public static void StrictlyIncreasing<T>(IEnumerable<T> source)
        where T : IComparable<T>
    {
        using var enumerator = source.GetEnumerator();

        if (!enumerator.MoveNext())
            return;

        var previous = enumerator.Current;

        while (enumerator.MoveNext())
        {
            if (enumerator.Current.CompareTo(previous) == 0)
                continue; // consecutive duplicate — collapse

            Assert.True(previous.CompareTo(enumerator.Current) < 0,
                $"watermark {previous} is not less than {enumerator.Current}");
            previous = enumerator.Current;
        }
    }
}
