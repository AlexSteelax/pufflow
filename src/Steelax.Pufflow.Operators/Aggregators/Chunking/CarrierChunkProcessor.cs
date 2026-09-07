using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Common;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;

namespace Steelax.Pufflow.Operators.Aggregators.Chunking;

/// <summary>
///     A pull-side chunker for a carrier stream: it accumulates the raw values of
///     <see cref="Carrier{T}" /> input items into an inner <see cref="Chunk{T}" />, tracks the maximum watermark
///     seen across the whole window (including bare progress items), and emits each chunk wrapped in a
///     <see cref="Carrier{ChunkOfT}" /> whose watermark is that window maximum. When a window closes with no data
///     but only progress (bare) items, an empty carrier is emitted so progress still moves forward.
/// </summary>
/// <typeparam name="T">The type of the accumulated element values.</typeparam>
/// <remarks>
///     Mirrors <see cref="ChunkProcessor{T,TChunk}" /> timing (size / linger end-of-stream) and reuses the same
///     wait/linger state machinery, projected onto <c>Carrier&lt;Chunk&lt;T&gt;&gt;</c>.
/// </remarks>
[Flow]
internal sealed partial class CarrierChunkProcessor<T> : IAsyncConsumator<Carrier<Chunk<T>>>
{
    private readonly Chunker<T> _chunker;
    private readonly TimeSpan _linger;
    private readonly int _size;

    private readonly CompleteSignal _signal;
    private readonly ITimer _timer;
    private readonly EventTask<bool> _await;

    private IAsyncConsumator<Carrier<T>> _source = null!;
    private CancellationToken _cancellationToken;

    private Watermark _windowMax = Watermark.Nothing();
    private bool _ready;

    public CarrierChunkProcessor(Chunker<T> chunker, int size, TimeSpan linger, TimeProvider? timeProvider = null)
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

    /// <summary>Wakes the consumer when the observed source wait completes.</summary>
    private void FireReadyResult() => _signal.Signal();

    public void Fuse(IAsyncConsumator<Carrier<T>> source, out IAsyncConsumator<Carrier<Chunk<T>>> target, FlowContext context)
    {
        _source = source;
        _cancellationToken = context.Token;
        target = this;
    }

    public bool TryRead([MaybeNullWhen(false)] out Carrier<Chunk<T>> value)
    {
        switch (TryHandleAwait())
        {
            case AwaitOutcome.Failed:
            case AwaitOutcome.EndOfStream:
                StopLinger();
                _signal.Complete();

                if (TryFlushWindow(out value, endOfStream: true))
                    return true;

                _ = _await.GetResult();
                value = default;
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

                if (TryFlushWindow(out value, endOfStream: false))
                    return true;
            }

            if (!_source.TryRead(out var item))
            {
                if (!_await.GetState().IsPending)
                    _ = _await.Observe(_source.WaitToReadAsync());

                switch (TryHandleAwait())
                {
                    case AwaitOutcome.DataReady:
                        continue;

                    case AwaitOutcome.EndOfStream:
                    case AwaitOutcome.Failed:
                        StopLinger();
                        _signal.Complete();

                        if (TryFlushWindow(out value, endOfStream: true))
                            return true;

                        _ = _await.GetResult();
                        value = default;
                        return false;
                }

                value = default;
                return false;
            }

            // Fold the item's watermark into the window maximum regardless of whether it carries data.
            if (item.Watermark > _windowMax)
                _windowMax = item.Watermark;

            if (!item.HasValue)
            {
                // A bare progress item advances the window watermark but does not occupy the chunk buffer.
                continue;
            }

            if (_chunker.IsEmpty)
            {
                _chunker.Rent(_size);
                StartLinger();
            }

            _ = _chunker.TryAdd(item.Value);
        }

        StopLinger();
        _signal.Complete();
        value = default;
        return false;
    }

    /// <summary>
    ///     Closes the current window: hands out the buffered data as <c>Carrier&lt;Chunk&lt;T&gt;&gt;</c> carrying the
    ///     window maximum, or — when only progress items arrived — an empty carrier with that watermark.
    /// </summary>
    private bool TryFlushWindow([MaybeNullWhen(false)] out Carrier<Chunk<T>> value, bool endOfStream)
    {
        if (_chunker.TryComplete(out var chunk))
        {
            value = new Carrier<Chunk<T>>(chunk, _windowMax);
            _windowMax = Watermark.Nothing();
            return true;
        }

        // No buffered data this window. Emit an empty carrier to carry the progress watermark only at the very
        // end of the stream (a chunk cannot usefully close mid-stream with nothing in it).
        if (endOfStream && !_windowMax.IsNothing)
        {
            value = new Carrier<Chunk<T>>(_windowMax);
            _windowMax = Watermark.Nothing();
            return true;
        }

        value = default;
        return false;
    }

    private enum AwaitOutcome
    {
        InFlight,
        DataReady,
        EndOfStream,
        Failed
    }

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

    public bool IsCompleted => _source.IsCompleted && _chunker.IsEmpty && !_await.GetState().IsPending;

    public ValueTask<bool> WaitToReadAsync()
    {
        while (true)
        {
            if (IsCompleted)
                return ValueTask.FromResult(false);

            if (_signal.TryReset())
                continue;

            return _signal.WaitAsync();
        }
    }
}
