namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     Defines a synchronous push producer. The input (write) side of a dataflow component.
/// </summary>
/// <typeparam name="T">The type of values to produce.</typeparam>
[PublicAPI]
public interface IProducator<in T>
    where T : allows ref struct
{
    /// <summary>Attempts to write a value without blocking.</summary>
    /// <param name="value">The value to write.</param>
    /// <returns><see langword="true" /> when the value was accepted; otherwise <see langword="false" />.</returns>
    bool TryWrite(T value);

    /// <summary>Attempts to signal the end of the stream, optionally carrying a fault.</summary>
    /// <param name="ex">The optional fault; <see langword="null" /> for successful completion.</param>
    /// <returns><see langword="true" /> when the stream was marked as completed; otherwise <see langword="false" />.</returns>
    bool TryComplete(Exception? ex = null);
}