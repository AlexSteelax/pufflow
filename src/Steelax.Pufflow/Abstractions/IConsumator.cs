using System.Diagnostics.CodeAnalysis;

namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     Defines a synchronous pull consumer. The output (read) side of a dataflow component.
/// </summary>
/// <typeparam name="T">The type of values to consume.</typeparam>
[PublicAPI]
public interface IConsumator<T>
    where T : allows ref struct
{
    /// <summary>Attempts to read a value without blocking.</summary>
    /// <param name="value">The read value, if available.</param>
    /// <returns><see langword="true" /> when a value was read; otherwise <see langword="false" />.</returns>
    bool TryRead([MaybeNullWhen(false)] out T value);
    
    /// <summary>
    /// 
    /// </summary>
    bool IsCompleted { get; }
}