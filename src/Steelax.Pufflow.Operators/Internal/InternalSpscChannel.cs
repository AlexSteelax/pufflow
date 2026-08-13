using System.Diagnostics.CodeAnalysis;
using Steelax.Toolkit.HighPerformance.Concurrency.Channels;

namespace Steelax.Pufflow.Operators.Internal;

/// <summary>
///     A thin adapter that exposes a bounded <see cref="SpscChannel{T}" /> through both flow interfaces at
///     once: the push (write) side as <see cref="IAsyncProducator{T}" /> and the pull (read) side as
///     <see cref="IAsyncConsumator{T}" />.
/// </summary>
/// <typeparam name="T">The type of buffered values.</typeparam>
/// <remarks>
///     <para>
///         Both interfaces delegate to the same underlying <see cref="SpscChannel{T}" />, so values written
///         through the producator side are read back through the consumator side in FIFO order. This is the
///         single relay that lets a push producer and a pull consumer share one bounded buffer without a
///         background pump.
///     </para>
///     <para>
///         The adapter is a <see langword="readonly" /> value type carrying a reference to the shared
///         channel; copying it yields another handle to the same channel. It is used internally by
///         <c>BypassBufferProcessor</c> to hand out the two sides of the buffer.
///     </para>
/// </remarks>
internal readonly record struct InternalSpscChannel<T>(SpscChannel<T> channel) :
    IAsyncProducator<T>,
    IAsyncConsumator<T>
{
    /// <inheritdoc cref="IProducator{T}.TryWrite" />
    public bool TryWrite(T value) => channel.TryWrite(value);

    /// <inheritdoc cref="IProducator{T}.TryComplete" />
    public bool TryComplete(Exception? ex = null) => channel.TryComplete(ex);

    /// <inheritdoc cref="IAsyncProducator{T}.WaitToWriteAsync" />
    public ValueTask<bool> WaitToWriteAsync() => channel.WaitToWriteAsync();


    /// <inheritdoc cref="IConsumator{T}.TryRead" />
    public bool TryRead([MaybeNullWhen(false)] out T value) => channel.TryRead(out value);

    /// <inheritdoc cref="IConsumator{T}.IsCompleted" />
    public bool IsCompleted => channel.IsCompleted;

    /// <inheritdoc cref="IAsyncConsumator{T}.WaitToReadAsync" />
    public ValueTask<bool> WaitToReadAsync() => channel.WaitToReadAsync();
}
