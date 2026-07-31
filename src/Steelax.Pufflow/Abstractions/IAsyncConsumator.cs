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
    /// <param name="completed">
    /// Set to <see langword="true"/> when the stream has ended; otherwise <see langword="false"/>.
    /// When the return value is <see langword="false"/> and <paramref name="completed"/> is
    /// <see langword="false"/>, no value is currently available but the stream is still active.
    /// </param>
    bool TryRead([MaybeNullWhen(false)] out T value, out bool completed);

    /// <summary>Waits asynchronously until a value is available or the stream ends.</summary>
    ValueTask WaitToReadAsync();
}
