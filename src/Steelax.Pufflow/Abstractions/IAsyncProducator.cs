namespace Steelax.Pufflow.Abstractions;

/// <summary>
///     Defines an asynchronous push producer. The async input (write) side of a dataflow component.
/// </summary>
/// <typeparam name="T">The type of values to produce.</typeparam>
[PublicAPI]
public interface IAsyncProducator<in T> : IProducator<T>
    where T : allows ref struct
{
    /// <summary>Waits asynchronously until the value can be written.</summary>
    /// <remarks>
    ///     The producer is the single writer: it signals completion itself through
    ///     <see cref="IProducator{T}.TryComplete" /> and then no longer calls
    ///     <see cref="IProducator{T}.TryWrite" />. A completion carrying a fault is rethrown here.
    /// </remarks>
    ValueTask<bool> WaitToWriteAsync();
}