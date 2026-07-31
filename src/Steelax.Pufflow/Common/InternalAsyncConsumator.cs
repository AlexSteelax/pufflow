using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Common;

/// <summary>
/// Wraps an <see cref="IAsyncConsumator{T}"/> instance to expose it via a standardized handler method.
/// </summary>
/// <typeparam name="T">The type of elements consumed.</typeparam>
/// <typeparam name="TAsyncConsumator">The concrete async consumator type.</typeparam>
/// <remarks>
/// Used internally by the runtime pipeline builder to wrap a resolved async consumator
/// into a uniform callable shape for compatibility with source-generated code.
/// </remarks>
internal readonly struct InternalAsyncConsumator<T, TAsyncConsumator>(TAsyncConsumator consumator)
    where TAsyncConsumator : IAsyncConsumator<T>
{
    /// <summary>
    /// Returns the wrapped async consumator instance.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token (currently unused but required for signature compatibility).</param>
    /// <returns>The underlying <typeparamref name="TAsyncConsumator"/> instance.</returns>
    public TAsyncConsumator GetAsyncConsumator(CancellationToken cancellationToken = default) => consumator;
}
