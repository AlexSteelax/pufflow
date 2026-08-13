using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Steelax.Toolkit.HighPerformance;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Aggregators.Chunking;

[Flow]
internal sealed partial class ChunkProcessor<T, TChunk> : IAsyncConsumator<TChunk>
{
    private readonly IChunkBuilder<T, TChunk> _chunker;
    private readonly TimeSpan _linger;
    private readonly int _size;

    private readonly CompleteSignal _signal;
    private readonly ITimer _timer;
    private readonly EventTask<bool> _await;
    
    private IAsyncConsumator<T> _source = null!;
    private CancellationToken _cancellationToken;
    private bool _ready;
    
    public ChunkProcessor(IChunkBuilder<T, TChunk> chunker, int size, TimeSpan linger, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(chunker);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(size);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(linger, TimeSpan.Zero);

        _chunker = chunker;
        _linger = linger;
        _size = size;
        _signal = new CompleteSignal();
        _await = new EventTask<bool>();
        _timer = (timeProvider ?? TimeProvider.System).CreateTimer(FireLingerSignal, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        _await.OnReady += FireReadyResult;
    }

    private void FireLingerSignal(object? _)
    {
        Volatile.Write(ref _ready, true);
        _signal.Signal();
    }

    /// <summary>
    ///     Wakes the consumer when the observed source wait completes. The outcome (data ready, end of
    ///     stream or fault) is resolved synchronously inside <see cref="TryRead" />, not from this async
    ///     continuation — an exception here would otherwise escape into the channel machinery.
    /// </summary>
    private void FireReadyResult() => _signal.Signal();
    
    public void Fuse(IAsyncConsumator<T> source, out IAsyncConsumator<TChunk> target, FlowContext context)
    {
        _source = source;
        _cancellationToken = context.Token;
        target = this;
    }

    public bool TryRead([MaybeNullWhen(false)] out TChunk chunk)
    {
        // A previously observed source wait finished while we were away (the async continuation only
        // woke us via the signal): resolve it now, before checking for a fresh chunk.
        switch (TryHandleAwait())
        {
            case AwaitOutcome.Failed:
            case AwaitOutcome.EndOfStream:
                StopLinger();
                _signal.Complete();
                
                if (_chunker.TryComplete(out chunk))
                    return true;

                _ = _await.GetResult();
                
                chunk = default!;
                return false;

            case AwaitOutcome.DataReady:
            case AwaitOutcome.InFlight:
                break;
        }
        
        while (!_cancellationToken.IsCancellationRequested)
        {
            if (_chunker.IsCompleted || Volatile.Read(ref _ready))
            {
                StopLinger();
                Volatile.Write(ref _ready, false);

                if (_chunker.TryComplete(out chunk))
                    return true;
            }
            
            if (!_source.TryRead(out var item))
            {
                if (!_await.GetState().IsPending)
                    _ = _await.Observe(_source.WaitToReadAsync());

                if (_source.IsCompleted)
                    _signal.Complete();

                chunk = default!;
                return false;
            }

            if (_chunker.IsEmpty)
            {
                _chunker.Rent(_size);
                StartLinger();
            }

            _ = _chunker.TryAdd(item);
        }

        _signal.Complete();
        chunk = default!;
        return false;
    }

    /// <summary>
    ///     The outcome of resolving an observed source wait in <see cref="TryHandleAwait" />.
    /// </summary>
    private enum AwaitOutcome
    {
        /// <summary>The wait is still in flight; no action required.</summary>
        InFlight,

        /// <summary>The wait resolved with data available; the source should be re-read.</summary>
        DataReady,

        /// <summary>The source stream is over; the accumulated chunk should be flushed.</summary>
        EndOfStream,
        
        Failed
    }

    /// <summary>
    ///     Resolves the outcome of an observed source wait (<see cref="_await" />), if it has completed.
    /// </summary>
    /// <remarks>
    ///     A fault or cancellation of the source wait propagates synchronously to the caller of
    ///     <see cref="TryRead" /> — it is not swallowed inside the async continuation.
    /// </remarks>
    private AwaitOutcome TryHandleAwait()
    {
        var state = _await.GetState();
        
        if (!state.IsCompleted)
            return AwaitOutcome.InFlight;

        if (!state.IsCompletedSuccessfully)
            return AwaitOutcome.Failed;

        return _await.GetResult() ? AwaitOutcome.DataReady : AwaitOutcome.EndOfStream;
    }
    
    private void StopLinger() => _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    private void StartLinger() => _timer.Change(_linger, Timeout.InfiniteTimeSpan);

    public bool IsCompleted => _source.IsCompleted && _chunker.IsEmpty;

    public ValueTask<bool> WaitToReadAsync()
    {
        while (true)
        {
            // The stream is over — no need to wait.
            if (IsCompleted)
                return ValueTask.FromResult(false);

            // A signal is raised but no room is available yet (it was consumed earlier): clear the
            // stale signal and re-check, so a signal raised between the check and the reset is not lost.
            if (_signal.TryReset())
                continue;

            // No signal raised: register a wait. The signal resolves to true (capacity freed) or false
            // (stream completed); a concurrently raised signal completes WaitAsync synchronously and the
            // loop re-checks the queue.
            return _signal.WaitAsync();
        }
    }
}