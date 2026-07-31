namespace Steelax.Pufflow.Common;

/// <summary>
/// Wraps an <see cref="IAsyncEnumerator{T}"/> instance to expose it via a standardized handler method.
/// </summary>
/// <typeparam name="T">The type of elements enumerated.</typeparam>
/// <typeparam name="TAsyncEnumerator">The concrete async enumerator type.</typeparam>
/// <remarks>
/// Used internally by the runtime pipeline builder to wrap a resolved async enumerator
/// into a uniform callable shape that accepts a <see cref="FlowContext"/> parameter.
/// </remarks>
internal readonly struct InternalAsyncEnumerator<T, TAsyncEnumerator>(TAsyncEnumerator enumerator)
    where TAsyncEnumerator : IAsyncEnumerator<T>
{
    /// <summary>
    /// Returns the wrapped async enumerator instance.
    /// </summary>
    /// <param name="_">The flow context (unused, but required for signature compatibility with source-generated code).</param>
    /// <returns>The underlying <typeparamref name="TAsyncEnumerator"/> instance.</returns>
    public TAsyncEnumerator GetAsyncEnumerator(FlowContext _) => enumerator;
}
