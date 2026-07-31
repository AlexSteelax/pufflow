namespace Steelax.Pufflow.Operators;

/// <summary>
/// Numeric and timing configuration for the <c>Warming</c> operator. Behavioural dependencies
/// (the job factory, key selector, policy and accumulator factory) are passed directly to the
/// <c>Warming</c> extension method — this type holds only tuning values.
/// </summary>
/// <remarks>
/// Defaults are chosen to be safe for small pipelines: single warm worker, a modest segment size
/// and a one-second linger. Adjust per deployment.
/// </remarks>
[PublicAPI]
public sealed record WarmOptions
{
    /// <summary>The maximum number of warm jobs running concurrently (1..32).</summary>
    [PublicAPI]
    public int MaxConcurrency { get; init; } = 4;

    /// <summary>The maximum number of segments buffered in the warmer's ring.</summary>
    [PublicAPI]
    public int MaxQueued { get; init; } = 32;

    /// <summary>The maximum number of keys per warming segment.</summary>
    [PublicAPI]
    public int SegmentCapacity { get; init; } = 4;

    /// <summary>The idle interval after which a partial warming segment is sealed.</summary>
    [PublicAPI]
    public TimeSpan SegmentLinger { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>The maximum total weight the per-key delayed buffers may hold.</summary>
    [PublicAPI]
    public long QueueWeightLimit { get; init; } = 100_000;

    /// <summary>
    /// The period of the recurring watchdog timer that periodically wakes a sleeping consumer loop so
    /// it re-checks the state (a safety net against a missed readiness signal). <see langword="null"/>
    /// or <see cref="Timeout.InfiniteTimeSpan"/> disables the watchdog.
    /// </summary>
    [PublicAPI]
    public TimeSpan WatchdogPeriod { get; init; } = Timeout.InfiniteTimeSpan;
}
