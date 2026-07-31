using System.Diagnostics.CodeAnalysis;

namespace Steelax.Pufflow.Abstractions;

/// <summary>
/// Defines an asynchronous pull consumer. The async output (read) side of a dataflow component.
/// </summary>
/// <typeparam name="T">The type of values to consume.</typeparam>
[PublicAPI]
public interface IAsyncConsumator<T>
    where T : allows ref struct
{
    /// <summary>Attempts to read a value without blocking.</summary>
    /// <param name="value">The read value, if available.</param>
    ReadResult TryRead([MaybeNullWhen(false)] out T value);

    /// <summary>Waits asynchronously until a value is available or the stream ends.</summary>
    ValueTask<bool> WaitToReadAsync();
}
