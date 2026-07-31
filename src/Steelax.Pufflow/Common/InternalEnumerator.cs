namespace Steelax.Pufflow.Common;

/// <summary>
/// Wraps an <see cref="IEnumerator{T}"/> instance to expose it via a standardized handler method.
/// </summary>
/// <typeparam name="T">The type of elements enumerated.</typeparam>
/// <typeparam name="TEnumerator">The concrete enumerator type.</typeparam>
/// <remarks>
/// Used internally by the runtime pipeline builder to wrap a resolved sync enumerator
/// into a uniform callable shape for compatibility with source-generated code.
/// </remarks>
internal readonly struct InternalEnumerator<T, TEnumerator>(TEnumerator enumerator)
    where TEnumerator : IEnumerator<T>
{
    /// <summary>
    /// Returns the wrapped enumerator instance.
    /// </summary>
    /// <returns>The underlying <typeparamref name="TEnumerator"/> instance.</returns>
    public TEnumerator GetEnumerator() => enumerator;
}
