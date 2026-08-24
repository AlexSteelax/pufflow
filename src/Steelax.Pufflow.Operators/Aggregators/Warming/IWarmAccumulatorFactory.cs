namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Creates <see cref="WarmAccumulator{TValue,TGroup}" /> instances for warming keys.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TValue">The input value type from the source stream.</typeparam>
/// <typeparam name="TGroup">The output group type produced by the accumulator.</typeparam>
/// <remarks>
///     The key is passed to <see cref="Create" /> so an implementation can produce a key-specific
///     accumulator (e.g. a different aggregation strategy per key).
/// </remarks>
[PublicAPI]
public interface IWarmAccumulatorFactory<in TKey, TValue, TGroup>
{
    /// <summary>Creates a warm accumulator for the specified <paramref name="key" />.</summary>
    /// <param name="key">The key the accumulator will store values for.</param>
    /// <returns>A new <see cref="WarmAccumulator{TValue,TGroup}" /> for <paramref name="key" />.</returns>
    WarmAccumulator<TValue, TGroup> Create(TKey key);
}

/// <summary>
///     Creates <see cref="WarmAccumulator{TValue,TGroup}" /> instances for warming keys.
/// </summary>
/// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
/// <typeparam name="TValue">The input value type from the source stream.</typeparam>
[PublicAPI]
public interface IWarmAccumulatorFactory<in TKey, TValue> : IWarmAccumulatorFactory<TKey, TValue, TValue>;