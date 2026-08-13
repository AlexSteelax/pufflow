namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Decides whether a key requires warming and observes when its warming completes.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TWarm"></typeparam>
/// <remarks>All members are called from the consumer loop thread and must be thread-safe.</remarks>
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
    ///     Called after the key's warm data has been applied and the key can be considered warmed.
    /// </summary>
    /// <param name="key">The key whose warming completed.</param>
    /// <param name="warm"></param>
    [PublicAPI]
    void OnWarmed(TKey key, TWarm warm);
}