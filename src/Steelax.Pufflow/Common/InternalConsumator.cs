using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Common;

/// <summary>
///     Wraps an <see cref="IConsumator{T}" /> instance to expose it via a standardized handler method.
/// </summary>
/// <typeparam name="T">The type of elements consumed.</typeparam>
/// <typeparam name="TConsumator">The concrete consumator type.</typeparam>
/// <remarks>
///     Used internally by the runtime pipeline builder to wrap a resolved sync consumator
///     into a uniform callable shape for compatibility with source-generated code.
/// </remarks>
internal readonly struct InternalConsumator<T, TConsumator>(TConsumator consumator)
    where TConsumator : IConsumator<T>
{
    /// <summary>
    ///     Returns the wrapped consumator instance.
    /// </summary>
    /// <returns>The underlying <typeparamref name="TConsumator" /> instance.</returns>
    public TConsumator Handle()
    {
        return consumator;
    }
}