namespace Steelax.Pufflow.Abstractions;

/// <summary>
/// Defines an asynchronous push producer. The async input (write) side of a dataflow component.
/// </summary>
/// <typeparam name="T">The type of values to produce.</typeparam>
[PublicAPI]
public interface IAsyncProducator<in T>
    where T : allows ref struct
{
    /// <summary>Attempts to write a value without blocking.</summary>
    bool TryWrite(T value);

    /// <summary>Waits asynchronously until the value can be written.</summary>
    /// <remarks>
    /// The producer is the single writer: it signals completion itself through <see cref="Complete"/>
    /// and then no longer calls <see cref="TryWrite"/>. A completion carrying a fault is rethrown here.
    /// </remarks>
    ValueTask WaitToWriteAsync();

    /// <summary>Marks the stream as complete. Optionally signals an error.</summary>
    /// <param name="ex">Optional exception that caused the completion.</param>
    void Complete(Exception? ex = null);
}
