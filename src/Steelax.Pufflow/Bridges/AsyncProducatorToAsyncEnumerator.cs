using System.Threading.Channels;
using Steelax.Pufflow.Abstractions;

namespace Steelax.Pufflow.Bridges;

/// <summary>
/// A bounded, buffered bridge from an <see cref="IAsyncProducator{T}"/> (the write side) to an
/// <see cref="IAsyncEnumerator{T}"/> (the read side), used to connect a producer loop to a downstream
/// consumer without building the buffer by hand. The buffering itself is delegated to a bounded
/// <see cref="Channel{T}"/> configured for a single reader and a single writer, so the producer never
/// overruns the configured <c>limit</c> and waits for a free slot on overflow.
/// </summary>
/// <typeparam name="T">The type of buffered values.</typeparam>
/// <remarks>
/// <para>
/// The bridge is single-producer/single-consumer by design: the producer observes <c>TryWrite</c> /
/// <c>WaitToWriteAsync</c>, the consumer drives <c>MoveNextAsync</c> / <c>Current</c>. Completion is
/// signaled through <c>Complete</c>: pending readers observe the end of stream, and a fault is
/// propagated through the channel to <c>MoveNextAsync</c>.
/// </para>
/// <para>
/// <see cref="TryWrite"/> never blocks; it returns <see langword="false"/> when the buffer is full or
/// the stream has been completed. <see cref="WaitToWriteAsync"/> and <see cref="MoveNextAsync"/> are
/// asynchronous and resume on the channel's continuations.
/// </para>
/// </remarks>
public sealed class AsyncProducatorToAsyncEnumerator<T> :
    IAsyncProducator<T>,
    IAsyncEnumerator<T>
{
    /// <summary>The bounded channel backing the buffer and the two readiness signals.</summary>
    private readonly Channel<T> _queue;

    /// <summary>The value produced by the last successful <see cref="MoveNextAsync"/>.</summary>
    private T _current = default!;

    /// <summary>
    /// Initializes a new buffered bridge.
    /// </summary>
    /// <param name="limit">The maximum number of buffered values (must be positive).</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="limit"/> is not positive.</exception>
    public AsyncProducatorToAsyncEnumerator(int limit)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        _queue = Channel.CreateBounded<T>(new BoundedChannelOptions(limit)
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = true
        });
    }

    #region Producator

    /// <summary>
    /// Attempts to enqueue a value without blocking.
    /// </summary>
    /// <param name="value">The value to buffer.</param>
    /// <returns>
    /// <see langword="true"/> when the value was buffered; otherwise <see langword="false"/> (the
    /// buffer is full, or the stream has already been completed via <see cref="Complete"/> and no more
    /// values can be written).
    /// </returns>
    public bool TryWrite(T value)
    {
        return _queue.Writer.TryWrite(value);
    }

    /// <summary>
    /// Waits asynchronously until a slot is free, without blocking the calling thread.
    /// </summary>
    /// <remarks>
    /// The producer is the single writer: it signals completion itself through <see cref="Complete"/>
    /// and then no longer calls <see cref="TryWrite"/>. After the stream is completed the wait completes
    /// (no more writes are possible).
    /// </remarks>
    public async ValueTask WaitToWriteAsync()
    {
        await _queue.Writer.WaitToWriteAsync();
    }

    /// <summary>
    /// Marks the stream as complete, waking any pending wait. A fault is propagated through the channel
    /// to <see cref="MoveNextAsync"/>.
    /// </summary>
    /// <param name="ex">Optional exception that caused the completion.</param>
    public void Complete(Exception? ex = null)
    {
        _queue.Writer.Complete(ex);
    }

    #endregion

    #region Enumerator

    /// <summary>Gets the value produced by the last successful <see cref="MoveNextAsync"/>.</summary>
    public T Current => _current;

    /// <summary>
    /// Advances to the next buffered value, waiting for one when the buffer is empty, without
    /// blocking the calling thread.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when <see cref="Current"/> holds the next value; <see langword="false"/>
    /// at the end of stream. A fault passed to <see cref="Complete"/> is propagated as an exception.
    /// </returns>
    public async ValueTask<bool> MoveNextAsync()
    {
        while (true)
        {
            if (_queue.Reader.TryRead(out var item))
            {
                _current = item;
                return true;
            }

            if (!await _queue.Reader.WaitToReadAsync())
            {
                _current = default!;
                return false;
            }
        }
    }

    /// <summary>Releases the bridge by completing the underlying channel.</summary>
    public ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }

    #endregion
}
