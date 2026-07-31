using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators;

/// <summary>
/// A pipe component that accumulates elements from an async source into pool-backed chunks and
/// emits them downstream as <see cref="Chunk{T}"/> instances.
/// </summary>
/// <typeparam name="T">The type of elements to chunk.</typeparam>
/// <remarks>
/// <para>
/// A chunk is emitted when either of the following conditions is met:
/// <list type="bullet">
/// <item><description>the chunk is full and rejects further elements (count trigger, driven by the
/// <see cref="Chunker{T}"/>);</description></item>
/// <item><description><c>linger</c> elapsed since the first element of the current chunk (time trigger).</description></item>
/// </list>
/// </para>
/// <para>
/// The time trigger is implemented by multiplexing the source's <c>MoveNextAsync</c> and a linger
/// timer through a <see cref="FanInSlim"/> (slot 0 = source, slot 1 = timer). This races an idle
/// source against the linger window without blocking or per-item task allocation, so a partial chunk
/// is flushed even when the source goes idle. When the source completes, any remaining partial chunk
/// is flushed downstream.
/// </para>
/// <para>
/// Ownership: a chunk yielded by this enumerator is owned by the consumer, which must dispose it to
/// return the underlying buffer to the pool. The processor returns the current (uncompleted) buffer
/// via the chunker when the pipeline tears down.
/// </para>
/// </remarks>
[Flow]
public sealed partial class ChunkProcessor<T>
{
    private const int SourceSlot = 0;
    private const int TimerSlot = 1;

    private readonly Chunker<T> _chunker;
    private readonly int _size;
    private readonly TimeSpan _linger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="ChunkProcessor{T}"/> class.
    /// </summary>
    /// <param name="chunker">The chunk builder used to accumulate and hand off chunks.</param>
    /// <param name="size">The number of elements requested when renting a chunk buffer.</param>
    /// <param name="linger">
    /// The maximum time to wait for additional elements before emitting a partial chunk.
    /// </param>
    /// <param name="timeProvider">
    /// The time provider used to schedule the linger timer; defaults to <see cref="TimeProvider.System"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="chunker"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="size"/> or <paramref name="linger"/> is not positive.
    /// </exception>
    public ChunkProcessor(Chunker<T> chunker, int size, TimeSpan linger, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(chunker);

        if (size <= 0)
            throw new ArgumentOutOfRangeException(nameof(size), "Chunk size must be a positive integer.");

        if (linger <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(linger), "Linger must be greater than zero.");

        _chunker = chunker;
        _size = size;
        _linger = linger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Starts the one-shot linger timer.</summary>
    private static void StartLinger(ITimer timer, TimeSpan linger)
        => timer.Change(linger, Timeout.InfiniteTimeSpan);

    /// <summary>
    /// Stops the linger timer and clears any pending timer signal.
    /// </summary>
    private static void StopLinger(ITimer timer, FanInSlim fanIn)
    {
        timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        fanIn.TryReset(TimerSlot);
    }

    /// <summary>
    /// Returns an async enumerator that chunks elements from <paramref name="source"/> into
    /// <see cref="Chunk{T}"/> instances.
    /// </summary>
    /// <param name="source">The upstream async enumerator to chunk.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    /// <returns>An async enumerator yielding <see cref="Chunk{T}"/> instances.</returns>
    /// <exception cref="OperationCanceledException">
    /// Thrown when the source operation is canceled.
    /// </exception>
    /// <exception cref="Exception">
    /// Rethrown when the source enumerator faults.
    /// </exception>
    public async IAsyncEnumerator<Chunk<T>> GetAsyncEnumerator(IAsyncEnumerator<T> source, FlowContext context)
    {
        var fanIn = new FanInSlim();
        var adapter = source.AsNonBlocking();
        adapter.OnReady += () => fanIn.Signal(SourceSlot);
        var timer = _timeProvider.CreateTimer(_ => fanIn.Signal(TimerSlot), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        var chunker = _chunker;

        try
        {
            chunker.Rent(_size);
            adapter.MoveNext();

            while (true)
            {
                await fanIn.WaitAsync();
                var slots = fanIn.Take();

                if (slots.IsSet(SourceSlot))
                {
                    var state = adapter.GetState();

                    TryFastWay:

                    if (state.IsCompletedSuccessfully)
                    {
                        var item = adapter.GetResult();

                        // A rejection means the chunk is full and will not accept more elements.
                        if (!chunker.TryAdd(item))
                        {
                            StopLinger(timer, fanIn);
                            slots = slots.Remove(TimerSlot, out _); // ignore a stale timer signal

                            chunker.TryComplete(out var chunk); // full chunk → consumer
                            yield return chunk;
                            chunker.Rent(_size);

                            chunker.TryAdd(item); // a fresh chunk always accepts its first element
                        }
                        else if (chunker.IsCompleted)
                        {
                            // The item filled the chunk → emit it immediately.
                            StopLinger(timer, fanIn);
                            slots = slots.Remove(TimerSlot, out _); // ignore a stale timer signal

                            chunker.TryComplete(out var chunk);
                            yield return chunk;
                            chunker.Rent(_size);
                        }

                        adapter.MoveNext();

                        state = adapter.GetState();

                        if (state.IsPending)
                        {
                            // Start the linger window from the first element of the current chunk.
                            if (!chunker.IsEmpty)
                                StartLinger(timer, _linger);
                        }
                        else
                        {
                            // The synchronous MoveNext already signaled SourceSlot; consume that
                            // signal so the loop does not re-enter this block while the next
                            // (async, in-flight) iteration is still pending.
                            fanIn.TryReset(SourceSlot);
                            goto TryFastWay;
                        }
                    }
                    else if (state.IsPending)
                    {
                        // The SourceSlot signal belongs to a synchronous step already consumed by the
                        // fast path (its callback may fire asynchronously). The current operation is
                        // still in flight → ignore the stale signal and keep waiting for its result.
                    }
                    else
                    {
                        // Terminal state: stop the linger timer, clear its signal, and emit the
                        // remaining chunk before closing. The fault originates in the source fetch,
                        // not in the accumulated data, so the data is delivered first.
                        StopLinger(timer, fanIn);

                        if (chunker.TryComplete(out var chunk)) // partial chunk → consumer
                            yield return chunk;

                        if (state.IsEndOfStream)
                            break;

                        if (state.IsFaulted)
                            throw adapter.Exception ?? new InvalidOperationException("The source enumerator faulted.");

                        if (state.IsCanceled)
                            throw new OperationCanceledException(context.Token);

                        // This code path must be unreachable.
                        throw new InvalidOperationException("The source enumerator returned an invalid state.");
                    }
                }

                if (slots.IsSet(TimerSlot))
                {
                    // Time elapsed while the source was idle → flush the partial chunk.
                    // The one-shot timer has already fired and Take() consumed the TimerSlot bit,
                    // so there is no pending signal or timer to stop before flushing.
                    if (chunker.TryComplete(out var chunk))
                    {
                        yield return chunk; // ownership → consumer
                        chunker.Rent(_size);
                    }
                }
            }
        }
        finally
        {
            chunker.Dispose(); // returns the current buffer to the pool if not completed
            timer.Dispose();
        }
    }
}
