// ReSharper disable CheckNamespace
// ReSharper disable ClassNeverInstantiated.Global

using JetBrains.Annotations;
using Xunit.Sdk;

namespace Xunit;

[PublicAPI]
public partial class Assert
{
    public static void WaitUntil(Func<bool> condition, CancellationToken cancellationToken, string? message = null)
    {
        var spinWait = new SpinWait();
        
        while (!cancellationToken.IsCancellationRequested)
        {
            if (condition.Invoke())
                return;
            
            spinWait.SpinOnce();
        }
        
        if (string.IsNullOrEmpty(message))
            cancellationToken.ThrowIfCancellationRequested();
        
        throw new XunitException(message);
    }

    /// <summary>
    ///     Asserts that after collapsing consecutive duplicates the range is strictly increasing.
    ///     Repeated values are allowed only as consecutive duplicates (e.g. the same watermark landing
    ///     in two adjacent windows); any non-increasing pair fails.
    /// </summary>
    /// <typeparam name="T">The element type; must be comparable with itself.</typeparam>
    /// <param name="source">The input range (order-sensitive).</param>
    /// <param name="strictly"></param>
    /// <param name="comparer"></param>
    public static void OrderIncreasing<T>(IEnumerable<T> source, bool strictly, Comparer<T>? comparer = null)
    {
        comparer ??= Comparer<T>.Default;
        
        using var enumerator = source.GetEnumerator();

        if (!enumerator.MoveNext())
            return;

        var previous = enumerator.Current;

        while (enumerator.MoveNext())
        {
            var current = enumerator.Current;
            var compare = comparer.Compare(previous, current);
            
            if (compare == 0 && strictly)
                throw new XunitException("consecutive duplicate — collapse");

            if (compare > 0)
                throw new XunitException($"watermark {previous} is not less than {current}");
            
            previous = enumerator.Current;
        }
    }
}