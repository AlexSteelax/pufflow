using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;
using Steelax.Toolkit.HighPerformance.Primitives;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Transforms an <see cref="IConsumer{TKey,TValue}" /> into a pipeline source emitting
///     <see cref="Carrier{T}" /> items whose payload is a <see cref="ConsumeResult{TKey,TValue}" />
///     (when a record was consumed) or an empty progress point carrying only a watermark (when the stream has
///     gone quiet with no data).
/// </summary>
/// <typeparam name="TKey">The Kafka message key type.</typeparam>
/// <typeparam name="TValue">The Kafka message value type.</typeparam>
/// <remarks>
///     <para>
///         The processor owns the progress state: it keeps a pool of windows
///         (<see cref="RingCursor{T}" /> of pre-created <see cref="WatermarkStore" />), and each emitted
///         message writes a <see cref="TopicPartitionEpoch" /> → offset entry into its window.
///     </para>
///     <para>
///         The reader (pipeline) only reports its own <see cref="Watermark" />: it publishes the newest value
///         via <see cref="SetReaderWatermark" /> into a single atomic field (<see cref="_publishedWatermark" />).
///         The consume loop reads that field on every watermark cutoff and derives a delayed commit point
///         (never flushing against the most recently published mark), then closes the current window and
///         flushes those closed head windows whose watermark the derived commit point has been reached.
///     </para>
///     <para>
///         The closed-window counter (<see cref="_closed" />) is bound to the pool: incremented when a
///         window is closed and decremented when it is flushed. The head window is considered closed while
///         the counter is greater than zero.
///     </para>
///     <para>
///         The emitted data item is the <see cref="ConsumeResult{TKey,TValue}" /> itself (no loss, no cost);
///         mapping to a domain object is done by a downstream pipeline operator. When consumption idles, an
///         empty <see cref="Carrier{T}" /> (bare progress) is emitted after a waiting cycle has elapsed with no
///         data, so the reader's watermark can still be advanced and committed offsets can close a tail of data
///         that has gone quiet.
///     </para>
/// </remarks>
[Flow]
internal sealed partial class KafkaConsumerProcessor<TKey, TValue> : IAsyncDisposable, IWatermarkCommiter
{
    /// <summary>The Kafka consumer.</summary>
    private readonly IConsumer<TKey, TValue> _consumer;

    /// <summary>
    ///     Processor settings. Stored as a value type (record struct) for fast access without copying and
    ///     without duplicate fields.
    /// </summary>
    private readonly KafkaConsumerOptions _options;

    /// <summary>
    ///     The watermark source for emitted messages. Defaults to monotonic time
    ///     (<see cref="WatermarkProvider.System" />); overridable for tests.
    /// </summary>
    private readonly WatermarkProvider _watermarkProvider;

    /// <summary>
    ///     The latest watermark reported by the reader (the pipeline). This is the single cross-thread state
    ///     updated by <see cref="SetReaderWatermark" />. It always reflects the newest reader progress and is not
    ///     itself the value windows are flushed against — the work loop derives a delayed "commit point" from it
    ///     so the newest (possibly still in-flight) progress step is never committed prematurely.
    /// </summary>
    private long _publishedWatermark = Watermark.NothingValue;

    /// <summary>
    ///     Records that could not be written to the output producer (it was not accepting them). A non-empty
    ///     queue means the loop must drain it first (a fresh watermark is obtained at write time) before
    ///     consuming the next records. Watermarks are not stored on failure.
    /// </summary>
    private readonly Deque<ConsumeResult<TKey, TValue>> _pending;

    /// <summary>
    ///     The most recent watermark that was actually emitted to the output producer (either as a record
    ///     item or as a bare progress marker). A bare marker is pushed only when the fresh provider tick has
    ///     advanced beyond this value by at least <see cref="KafkaConsumerOptions.WatermarkAdvanceInterval" />,
    ///     so progress resets happen periodically — keeping the merged stream's watermark alive — without
    ///     spamming the output on every empty poll.
    /// </summary>
    private long _lastEmittedWatermark = Watermark.NothingValue;

    /// <summary>The shared loop signal multiplexer (advance/watermark timers).</summary>
    private readonly FanInSlim _fan;

    /// <summary>The offset advance strategy (OffsetStore one-by-one / ManualCommit in bulk).</summary>
    private readonly KafkaAdvanceStrategy _advanceStrategy;

    /// <summary>The one-shot advance timer (window commit / leaving emergency-idle).</summary>
    private readonly ITimer _advanceTimer;

    /// <summary>The periodic watermark timer (cutoffs: close window + flush).</summary>
    private readonly ITimer _watermarkTimer;

    private readonly KafkaErrorPolicy _errorPolicy;

    private IProducator<Carrier<ConsumeResult<TKey, TValue>>> _buffer = null!;

    private const int AdvanceTimerSignal = 0;
    private const int WatermarkTimerSignal = 2;

    /// <summary>Initializes a new instance of the processor.</summary>
    /// <param name="consumer">The Kafka consumer. After being passed to the processor, external access is forbidden.</param>
    /// <param name="options">Processor settings (buffer capacity, window pool size, backpressure threshold, etc.).</param>
    /// <param name="errorPolicy">The error policy deciding how consume and advance (commit) errors are handled.</param>
    /// <param name="watermarkProvider">The watermark source; defaults to monotonic time.</param>
    /// <param name="timeProvider">The time source for the timers; defaults to the system one.</param>
    public KafkaConsumerProcessor(
        IConsumer<TKey, TValue> consumer,
        KafkaConsumerOptions options,
        KafkaErrorPolicy? errorPolicy,
        WatermarkProvider? watermarkProvider = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(consumer);

        _consumer = consumer;
        _options = options;
        _errorPolicy = errorPolicy ?? KafkaErrorPolicy.Default;
        _watermarkProvider = watermarkProvider ?? WatermarkProvider.System;

        _windows = new RingCursor<WatermarkStore>(options.WindowSize, static () => new WatermarkStore());
        _pending = new Deque<ConsumeResult<TKey, TValue>>(options.EmergencyCapacity);
        _fan = new FanInSlim();
        _advanceStrategy = KafkaAdvanceStrategy.Create(consumer, options.AdvanceStrategy);

        _advanceTimer = (timeProvider ?? TimeProvider.System)
            .CreateTimer(static state => Unsafe.As<object?, FanInSlim>(ref state).Signal(AdvanceTimerSignal), _fan, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _watermarkTimer = (timeProvider ?? TimeProvider.System)
            .CreateTimer(static state => Unsafe.As<object?, FanInSlim>(ref state).Signal(WatermarkTimerSignal), _fan, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    ///     Starts pushing carrier items (records or bare progress) into a synchronous producer, launching the
    ///     background loop on the thread pool.
    /// </summary>
    /// <param name="target">The synchronous producer receiving the emitted carriers.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    /// <returns>The background consume task.</returns>
    [PublicAPI]
    public void Fuse(IProducator<Carrier<ConsumeResult<TKey, TValue>>> target, FlowContext context)
    {
        _buffer = target;
        
        _ = context.RegisterBackground(() => InternalExecuteAsync(context));

        context.RegisterDisposable(this);
    }

    /// <summary>
    ///     Publishes the reader's (pipeline) watermark. Safe to call from any thread.
    /// </summary>
    /// <param name="watermark">The mark up to which the reader has processed data.</param>
    /// <remarks>
    ///     <para>
    ///         A <see cref="Watermark.Nothing()" /> value is ignored — it carries no progress.
    ///     </para>
    ///     <para>
    ///         Only one field (<see cref="_publishedWatermark" />) is advanced, as the maximum of the reported
    ///         watermarks. The work loop turns these values into a delayed commit point (windows are committed
    ///         against a previous such value, never against the most recently published one), which lets a whole
    ///         significant step elapse before confirmed offsets are flushed.
    ///     </para>
    /// </remarks>
    [PublicAPI]
    public void SetReaderWatermark(Watermark watermark)
    {
        // Nothing carries no progress — ignore it.
        if (watermark.IsNothing)
            return;

        var current = (long)watermark;

        // Publish the newest progress point only; repeated (or lower) values are naturally ignored.
        InterlockedMath.AdvanceMax(ref _publishedWatermark, current);
    }

    /// <summary>Returns the latest watermark reported by the reader (for tests/diagnostics).</summary>
    [PublicAPI]
    public Watermark GetReaderWatermark() => Watermark.From(Volatile.Read(ref _publishedWatermark));

    /// <summary>
    ///     Flushes the closed head windows whose watermark has been reached by the reader's confirmed
    ///     commit point (<paramref name="commitWatermark" />).
    /// </summary>
    /// <param name="commitWatermark">The confirmed reader commit point the windows are flushed against.</param>
    /// <remarks>
    ///     Walks the closed-window pool from the head: for each closed window, if the commit point reaches the
    ///     window watermark, the window is flushed and its slot is released (reused). If a window is not yet
    ///     confirmed, the pass stops — no further windows are flushed. The caller owns the delayed commit-point
    ///     derivation; this method never reads shared reader state.
    /// </remarks>
    private void FlushReadyWindows(Watermark commitWatermark)
    {
        while (Volatile.Read(ref _closed) > 0 && _windows.PeekFirst(out var headIndex))
        {
            ref var head = ref _windows[headIndex];

            if (commitWatermark < head.Watermark)
                break;

            // The window is closed and the reader confirmed it — commit and release the slot. On a commit
            // error the policy decides the action: continue (the window stays closed and is retried on the
            // next cutoff), abort (stop the loop cleanly) or throw (fault the pipeline).
            try
            {
                head.Flush(_advanceStrategy);
            }
            catch (KafkaException ex)
            {
                switch (_errorPolicy.OnAdvanceError(ex))
                {
                    case KafkaErrorAction.Continue:
                        Trace.WriteLine($"KafkaConsumerProcessor: advance error suppressed ({ex.Error.Code}) the closed window will be retried on the next watermark cutoff.");
                        break;

                    case KafkaErrorAction.Abort:
                        throw new KafkaLoopAbortException();

                    case KafkaErrorAction.Throw:
                    default:
                        throw;
                }
            }

            Interlocked.Decrement(ref _closed);

            // The slot is reused: the window was cleared by Flush, the window shifts toward the head.
            _windows.ShrinkFirst();
        }
    }

    /// <summary>
    ///     Advances the pipeline by one step: drains the pending queue into the output producer, then
    ///     either writes a freshly polled record directly or buffers it in the pending queue.
    /// </summary>
    /// <returns>
    ///     <see langword="true" /> when a record was consumed and emitted (and recorded in the window);
    ///     <see langword="false" /> when no data is available or the producer is not accepting (the record
    ///     is kept in <see cref="_pending" /> for a retry).
    /// </returns>
    /// <remarks>
    ///     The pending queue is drained before polling new data, so previously unsent records are never
    ///     lost or reordered. Recording into the window happens only after a successful write, so a record
    ///     that could not be emitted is never marked as progress.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Advance()
    {
        ref var window = ref TakeWindow();

        // Try to drain the pending queue into the output producer.
        while (_pending.TryPeekFirst(out var result))
        {
            var watermark = _watermarkProvider.GetWatermark();

            if (!TryWriteRecord(result, watermark))
                break;

            window.Add(watermark, TopicPartitionEpoch.From(result), result.Offset);
            _ = _pending.TryPopFirst(out _);
        }

        // Direct write into the output stream.
        if (_pending.IsEmpty)
        {
            if (PollConsumer() is not { } result)
                return false;

            var watermark = _watermarkProvider.GetWatermark();

            if (!TryWriteRecord(result, watermark))
            {
                // The producer is not accepting — keep the record in the pending queue.
                _ = _pending.TryAddLast(result);
                return false;
            }

            window.Add(watermark, TopicPartitionEpoch.From(result), result.Offset);
            return true;
        }

        // Keep polling: since the direct-write branch failed, buffer the records in the pending queue.
        if (!_pending.IsFull)
        {
            if (PollConsumer() is { } result)
                _ = _pending.TryAddLast(result);
        }

        return false;
    }

    /// <summary>
    ///     Attempts to write an empty progress carrier when the stream has gone quiet, so the reader watermark is
    ///     reset periodically and the merged stream's watermark (the minimum across sources) keeps advancing.
    /// </summary>
    /// <returns>
    ///     <see langword="true" /> when the empty progress carrier was emitted;
    ///     <see langword="false" /> when it was not needed or the output was full.
    /// </returns>
    /// <remarks>
    ///     The empty carrier is never recorded into a window and never buffered in <see cref="_pending" /> — it is
    ///     idempotent and, on a full output, the loop simply retries on the next idle wake.
    ///     <para />
    ///     A marker is pushed only when the fresh provider tick has advanced beyond the last emitted watermark by at
    ///     least <see cref="KafkaConsumerOptions.WatermarkAdvanceInterval" />. This yields periodic progress resets,
    ///     not a one-shot marker per quiet period: if this source goes idle while another merged source keeps
    ///     flowing, its watermark — the minimum of the two — would otherwise freeze at the stale value and block
    ///     the other source's window commits.
    /// </remarks>
    private bool TryEmitIdleWatermark()
    {
        // The fresh tick must have advanced beyond the last emitted progress by at least the configured
        // interval. This throttles the marker cadence while keeping the merged watermark alive.
        var fresh = _watermarkProvider.GetWatermark();
        var last = Volatile.Read(ref _lastEmittedWatermark);

        if (fresh - last < (long)_options.WatermarkAdvanceInterval.TotalMilliseconds)
            return false;

        // Every record written into a window reconciled its watermark into _lastEmittedWatermark, so the
        // floating window's watermark never exceeds last — and passing the threshold guarantees fresh > last.
        // The marker is simply the fresh tick advanced by one minimal step: strictly newer than both the last
        // emitted progress and the floating window.
        var watermark = fresh.Add(TimeSpan.FromMilliseconds(1));

        if (!TryWriteMarker(watermark))
            return false;

        // The marker was accepted — record the progress point.
        ReconcileEmittedWatermark(watermark);
        return true;
    }

    /// <summary>Builds and writes a carrier carrying a consumed record as its data.</summary>
    private bool TryWriteRecord(ConsumeResult<TKey, TValue> result, Watermark watermark)
    {
        if (!_buffer.TryWrite(new Carrier<ConsumeResult<TKey, TValue>>(result, watermark)))
            return false;

        ReconcileEmittedWatermark(watermark);
        return true;
    }

    /// <summary>Builds and writes an empty carrier (bare progress) with only a watermark and no data.</summary>
    private bool TryWriteMarker(Watermark watermark)
    {
        return _buffer.TryWrite(new Carrier<ConsumeResult<TKey, TValue>>(watermark));
    }

    /// <summary>
    ///     Records <paramref name="watermark" /> as the newest emitted progress point. Owned exclusively by the
    ///     consume loop (single-threaded), so it is a plain field write; <see cref="SetReaderWatermark" /> is the
    ///     only cross-thread entry point and the only place that needs an atomic advance.
    /// </summary>
    private void ReconcileEmittedWatermark(Watermark watermark)
    {
        if (watermark > _lastEmittedWatermark)
            _lastEmittedWatermark = watermark;
    }

    /// <summary>Performs a non-blocking poll, returning the next record or <see langword="null" />.</summary>
    private ConsumeResult<TKey, TValue>? PollConsumer()
    {
        try
        {
            var ret = _consumer.Consume(millisecondsTimeout: 0);

            if (ret is { IsPartitionEOF: false })
                return ret;
        }
        catch (KafkaException ex)
        {
            // The error policy decides the action: continue (treat as an empty poll and retry), abort
            // (stop the loop cleanly) or throw (fault the pipeline).
            switch (_errorPolicy.OnConsumeError(ex))
            {
                case KafkaErrorAction.Continue:
                    Trace.WriteLine($"KafkaConsumerProcessor: consume error suppressed ({ex.Error.Code}); treated as an empty poll and retried.");
                    return null;

                case KafkaErrorAction.Abort:
                    throw new KafkaLoopAbortException();

                case KafkaErrorAction.Throw:
                default:
                    throw;
            }
        }

        return null;
    }

    /// <summary>
    ///     Determines whether the system is in emergency mode (slow consumption): the pending queue is not
    ///     empty, i.e. the output producer is not accepting records.
    /// </summary>
    private bool IsEmergency => !_pending.IsEmpty;

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _advanceTimer.DisposeAsync();
        await _watermarkTimer.DisposeAsync();
    }
}
