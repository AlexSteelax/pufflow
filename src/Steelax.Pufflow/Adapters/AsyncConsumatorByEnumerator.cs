using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using Steelax.Pufflow.Abstractions;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Adapters;

/// <summary>
///     Adapts a non-blocking <see cref="IAsyncEnumerator{T}" /> into an <see cref="IAsyncConsumator{T}" />.
///     Values are exposed as they become ready; the enumerator's end-of-stream, a fault, and the supplied
///     cancellation token all translate into the consumator's completion contract.
/// </summary>
/// <typeparam name="T">The element type flowing through the stream.</typeparam>
public sealed class AsyncConsumatorByEnumerator<T> : IAsyncConsumator<T>, IAsyncDisposable
{
    private readonly EventEnumerator<T> _enumerator;
    private readonly CompleteSignal _signal;
    private readonly CancellationTokenRegistration _registration;
    private int _disposed;

    /// <summary>
    ///     Wraps the enumerator and listens for readiness and cancellation.
    /// </summary>
    /// <param name="enumerator">The enumerator adapted into a consumator. Owned by this instance once passed.</param>
    /// <param name="cancellationToken">When cancelled, the stream is marked completed and waiters are released.</param>
    public AsyncConsumatorByEnumerator(IAsyncEnumerator<T> enumerator, CancellationToken cancellationToken)
    {
        _signal = new CompleteSignal();
        _enumerator = enumerator.AsNonBlocking();

        _registration = cancellationToken.Register(() => _signal.Complete());
        
        _enumerator.OnReady += _signal.Signal;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CompleteEos()
    {
        if (!_signal.IsCompleted)
            _signal.Complete();
    }

    /// <inheritdoc/>
    public bool TryRead([MaybeNullWhen(false)] out T value)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            value = default;
            return false;
        }

        Recheck:

        var state = _enumerator.GetState();

        if (state.IsEndOfStream)
        {
            CompleteEos();
            value = default;
            return false;
        }

        if (state.IsPending)
        {
            value = default;
            return false;
        }

        if (state.IsCompleted)
        {
            try
            {
                value = _enumerator.GetResult();
            }
            catch (OperationCanceledException)
            {
                // The routed move ended with a cancellation (either the underlying operation was canceled
                // or the source surfaced a cancellation fault): treat it as completion, not a data error.
                CompleteEos();
                value = default;
                return false;
            }

            // Advance the enumerator past the consumed value so a subsequent read observes the following
            // element (GetResult alone does not consume; the value is re-read otherwise).
            _enumerator.MoveNext(OnCompletedBehavior.SkipCallbackIfCompleted);

            return true;
        }

        if (_enumerator.MoveNext(OnCompletedBehavior.SkipCallbackIfCompleted))
            goto Recheck;

        value = default;
        return false;
    }

    /// <inheritdoc/>
    public bool IsCompleted => _signal.IsCompleted;

    /// <inheritdoc/>
    public ValueTask<bool> WaitToReadAsync()
    {
        if (Volatile.Read(ref _disposed) != 0)
            return ValueTask.FromResult(false);

        Recheck:

        var state = _enumerator.GetState();

        // End of stream, a canceled move and a cancellation fault are all terminal for the reader:
        // report completion instead of readiness so a consumer never busy-loops on TryRead=false.
        if (state.IsEndOfStream
            || state.IsCanceled
            || (state.IsFaulted && _enumerator.Exception is OperationCanceledException))
        {
            CompleteEos();
            return ValueTask.FromResult(false);
        }

        if (state.IsCompleted)
            return ValueTask.FromResult(true);

        if (state.IsPending)
            return _signal.WaitAsync();

        if (_enumerator.MoveNext(OnCompletedBehavior.SkipCallbackIfCompleted))
            goto Recheck;

        return _signal.WaitAsync();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        // Guard against a double disposal: the underlying enumerator may not be safely disposed twice.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // Release consumers parked on WaitToReadAsync: the terminal signal dominates subsequent reads,
        // so consumers observe end-of-stream and stop touching the enumerator.
        _signal.Complete();

        // Let any in-flight enumerator move settle to a terminal state before disposing it. Disposing an
        // asynchronous iterator that is still suspended on an in-flight await throws NotSupportedException;
        // the caller is expected to complete (or cancel) the source so the move resolves.
        for (var spins = 0; spins < 256 && _enumerator.GetState().IsPending; spins++)
            await Task.Yield();

        try
        {
            await _enumerator.DisposeAsync();
        }
        catch (NotSupportedException)
        {
            // The underlying async iterator was still suspended on an in-flight move (a .NET disposal
            // limitation for async iterators). The stream is already released: consumers were woken by the
            // signal and the resources settle once the source completes. Swallowing here keeps teardown safe.
        }

        await _registration.DisposeAsync();
    }
}