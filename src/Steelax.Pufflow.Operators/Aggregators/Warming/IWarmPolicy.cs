namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Decides whether a key requires warming and observes when its warming completes.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
/// <remarks>
///     All members are called from the consumer loop thread and must be thread-safe. <see cref="OnWarmed(TKey,TWarm)" /> or <see cref="OnWarmed(TKey)" />
///     is invoked exactly once for every key of a completed segment: with warm data when the job produced
///     any, or without data when it did not.
/// </remarks>
[PublicAPI]
public interface IWarmPolicy<in TKey, in TWarm>
{
    /// <summary>
    ///     Determines whether the specified <paramref name="key" /> requires warming before its values
    ///     are forwarded downstream.
    /// </summary>
    /// <param name="key">The key to check.</param>
    /// <returns><see langword="true" /> if the key requires warming; otherwise, <see langword="false" />.</returns>
    [PublicAPI]
    bool ShouldWarm(TKey key);

    /// <summary>
    ///     Called after the key's warm data has been applied: the key produced <paramref name="warm" /> and
    ///     can be considered warmed with that data.
    /// </summary>
    /// <param name="key">The key whose warming completed.</param>
    /// <param name="warm">The warm data produced for the key.</param>
    [PublicAPI]
    void OnWarmed(TKey key, TWarm warm);
    
    /// <summary>
    ///     Called when the key's warming completed without producing warm data (the job recorded no result
    ///     for the key). The key can still be considered warmed, just without payload.
    /// </summary>
    /// <param name="key">The key whose warming completed.</param>
    [PublicAPI]
    void OnWarmed(TKey key);
}