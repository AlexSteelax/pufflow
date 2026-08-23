namespace Steelax.Pufflow.Operators.Tests;

/// <summary>
///     Shared helpers for operator tests.
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
}
