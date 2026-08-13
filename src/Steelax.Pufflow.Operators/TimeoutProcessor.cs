using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     A pipe component that races each upstream <c>MoveNextAsync</c> against a timeout and emits
///     <see cref="Unio{T, AwaitTimeout}" /> values downstream: the element when it arrives in time, or
///     an <see cref="AwaitTimeout" /> marker when the timeout elapses while the source is idle.
/// </summary>
/// <typeparam name="T">The type of elements flowing through the timeout.</typeparam>
/// <remarks>
///     <para>
///         Every wait for the source is given its own timeout window. The window is armed just before the
///         wait and re-armed after each yielded element or timeout marker, so the consumer may spend an
///         unbounded amount of time processing a value without tripping the timeout.
///     </para>
///     <para>
///         The race is implemented by multiplexing the source's <c>MoveNextAsync</c> and a timeout timer
///         through a <see cref="FanInSlim" /> (slot 0 = source, slot 1 = timer), mirroring
///         <see cref="Aggregators.Chunking.ChunkProcessor{T,TChunk}" />. When the source completes, the enumerator closes normally;
///         when it faults or is canceled, the fault or <see cref="OperationCanceledException" /> propagates
///         downstream.
///     </para>
/// </remarks>
[Flow]
public sealed partial class TimeoutProcessor<T>
{
    private const int SourceSlot = 0;
    private const int TimerSlot = 1;

    private readonly TimeSpan _timeout;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TimeoutProcessor{T}" /> class.
    /// </summary>
    /// <param name="timeout">
    ///     The maximum time to wait for an element before emitting an <see cref="AwaitTimeout" /> marker.
    /// </param>
    /// <param name="timeProvider">
    ///     The time provider used to schedule the timeout timer; defaults to <see cref="TimeProvider.System" />.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="timeout" /> is not positive.
    /// </exception>
    public TimeoutProcessor(TimeSpan timeout, TimeProvider? timeProvider = null)
    {
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

        _timeout = timeout;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Starts the one-shot timeout timer.</summary>
    private static void StartTimeout(ITimer timer, TimeSpan timeout)
    {
        timer.Change(timeout, Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    ///     Stops the timeout timer and clears any pending timer signal.
    /// </summary>
    private static void StopTimeout(ITimer timer, FanInSlim fanIn)
    {
        timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        fanIn.TryReset(TimerSlot);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="source"></param>
    /// <param name="output"></param>
    /// <param name="context"></param>
    public void Fuse(IAsyncEnumerator<T> source, out IAsyncEnumerator<Unio<T, AwaitTimeout>> output, FlowContext context)
    {
        output = GetAsyncEnumerator(source, context);
    }

    /// <summary>
    ///     Returns an async enumerator that wraps <paramref name="source" /> with a per-wait timeout.
    /// </summary>
    /// <param name="source">The upstream async enumerator to time out.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    /// <returns>
    ///     An async enumerator yielding <see cref="Unio{T, AwaitTimeout}" /> values; <c>T0</c> holds an
    ///     element delivered in time, <c>T1</c> holds an <see cref="AwaitTimeout" /> marker.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    ///     Thrown when the source operation is canceled.
    /// </exception>
    /// <exception cref="Exception">
    ///     Rethrown when the source enumerator faults.
    /// </exception>
    public async IAsyncEnumerator<Unio<T, AwaitTimeout>> GetAsyncEnumerator(IAsyncEnumerator<T> source, FlowContext context)
    {
        var fanIn = new FanInSlim();
        var adapter = source.AsNonBlocking();
        adapter.OnReady += () => fanIn.Signal(SourceSlot);
        var timer = _timeProvider.CreateTimer(_ => fanIn.Signal(TimerSlot), null, Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);

        try
        {
            adapter.MoveNext();
            StartTimeout(timer, _timeout);

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

                        StopTimeout(timer, fanIn);

                        yield return item;

                        adapter.MoveNext();

                        state = adapter.GetState();

                        if (state.IsPending)
                        {
                            StartTimeout(timer, _timeout); // re-arm the window for the next wait
                            continue; // skip a stale TimerSlot signal in this iteration
                        }

                        // The synchronous MoveNext already signaled SourceSlot; consume that
                        // signal so the loop does not re-enter this block while the next
                        // (async, in-flight) iteration is still pending.
                        fanIn.TryReset(SourceSlot);
                        goto TryFastWay;
                    }

                    if (state.IsPending)
                    {
                        // The SourceSlot signal belongs to a synchronous step already consumed by the
                        // fast path (its callback may fire asynchronously). The current operation is
                        // still in flight → ignore the stale signal and keep waiting for its result.
                    }
                    else
                    {
                        // Terminal state: stop the timer and propagate completion, fault, or cancel.
                        StopTimeout(timer, fanIn);

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
                    // Timeout elapsed while the source was idle → emit the marker and keep waiting.
                    // The one-shot timer has already fired and Take() consumed the TimerSlot bit,
                    // so there is no pending signal or timer to stop before re-arming.
                    yield return default(AwaitTimeout);
                    StartTimeout(timer, _timeout);
                }
            }
        }
        finally
        {
            timer.Dispose();
        }
    }
}