namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Atomic compare-and-swap helpers for monotonically advancing numeric state.
/// </summary>
internal static class InterlockedMath
{
    /// <summary>
    ///     Atomically advances <paramref name="location" /> to <c>max(location, value)</c>.
    /// </summary>
    /// <param name="location">The field to advance. Read and written with <see cref="Volatile" /> semantics.</param>
    /// <param name="value">The value to promote the field to.</param>
    /// <returns>The field's value observed by the winning operation.</returns>
    /// <remarks>
    ///     If the field already holds a value greater than or equal to <paramref name="value" />, no write
    ///     happens and the current value is returned — the caller's contribution is a no-op.
    /// </remarks>
    public static long AdvanceMax(ref long location, long value)
    {
        while (true)
        {
            var comparand = Volatile.Read(ref location);

            if (comparand >= value)
                return comparand;

            var next = Interlocked.CompareExchange(ref location, value, comparand);

            if (next == comparand)
                return next;
        }
    }

    /// <summary>
    ///     Atomically advances <paramref name="location" /> to <c>max(location, value)</c>, seeding the
    ///     compare-and-swap with a caller-provided <paramref name="comparand" /> (e.g. a value read earlier
    ///     by the caller, or the outcome of a previous phase).
    /// </summary>
    /// <param name="location">The field to advance. Read and written with <see cref="Volatile" /> semantics.</param>
    /// <param name="value">The value to promote the field to.</param>
    /// <param name="comparand">The expected current value; re-read on contention.</param>
    /// <returns>The field's value observed by the winning operation.</returns>
    /// <remarks>
    ///     The caller-provided <paramref name="comparand" /> is only a hint: the actual field is always
    ///     re-read before the write, so a stale or lower comparand can never cause a value regression.
    /// </remarks>
    public static long AdvanceMax(ref long location, long value, long comparand)
    {
        while (true)
        {
            var next = Interlocked.CompareExchange(ref location, value, comparand);

            if (next == comparand)
                return next;
            
            comparand = Volatile.Read(ref location);
            
            if (comparand >= value)
                return comparand;
        }
    }
}