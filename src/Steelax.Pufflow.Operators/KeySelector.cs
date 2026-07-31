namespace Steelax.Pufflow.Operators;

/// <summary>
/// Extracts a key from a source value.
/// </summary>
/// <typeparam name="TValue">WarmSource type. Contravariant.</typeparam>
/// <typeparam name="TKey">Key type. Covariant.</typeparam>
/// <param name="source">The source value.</param>
/// <returns>The extracted key.</returns>
public delegate TKey KeySelector<TValue, out TKey>(scoped in TValue source);
