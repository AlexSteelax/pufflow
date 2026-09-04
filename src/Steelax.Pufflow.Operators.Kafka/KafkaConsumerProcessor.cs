using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Toolkit.HighPerformance.Concurrency.Primitives;
using Steelax.Toolkit.HighPerformance.Primitives;
using Unio;

namespace Steelax.Pufflow.Operators.Kafka;

/// <summary>
///     Transforms an <see cref="IConsumer{TKey,TValue}" /> into a pipeline source emitting
///     <see cref="Watermarked{T}" /> items whose payload is a union of a
///     <see cref="ConsumeResult{TKey,TValue}" /> (branch T0) or a bare <see cref="Unit" /> marker
///     (branch T1) carrying a progress watermark with no data.
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
///         The reader (pipeline) only reports its own <see cref="Watermark" />: it publishes it via
///         <see cref="SetReaderWatermark" /> (cast to <see cref="long" /> + <see cref="Interlocked" />).
///         The consume loop reads the field via <see cref="Volatile" /> and, once the reader's watermark
///         exceeds the watermark of a closed head window, flushes (commits) that window.
///     </para>
///     <para>
///         The closed-window counter (<see cref="_closed" />) is bound to the pool: incremented when a
///         window is closed and decremented when it is flushed. The head window is considered closed while
///         the counter is greater than zero.
///     </para>
///     <para>
///         The emitted value item is the <see cref="ConsumeResult{TKey,TValue}" /> itself (no loss, no cost);
///         mapping to a domain object is done by a downstream pipeline operator. When consumption idles, a
///         single bare <see cref="Unit" /> progress marker is emitted after a waiting cycle has elapsed with
///         no data, so the reader's watermark can still be advanced and committed offsets can close a tail of
///         data that has gone quiet.
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
    ///     The reader's confirmed commit point: the watermark up to which the pipeline has fully
    ///     processed all records. Windows whose watermark does not exceed this value are flushed.
    ///     Updated by <see cref="SetReaderWatermark" /> when the reader breaks the current
    ///     <see cref="_thresholdWatermark" />. Read via <see cref="Volatile" /> by
    ///     <see cref="FlushReadyWindows" />.
    /// </summary>
    private long _readerWatermark = Watermark.NothingValue;

    /// <summary>
    ///     The current progress threshold: the last watermark reported by the reader that has not yet been
    ///     promoted to <see cref="_readerWatermark" />. The reader must report a strictly greater watermark
    ///     to break it — the promoted value (the old threshold) becomes the commit point, and the new value
    ///     becomes the next threshold. This way repeated watermark values (same clock tick) never advance
    ///     the commit point prematurely. Written by <see cref="SetReaderWatermark" />.
    /// </summary>
    private long _thresholdWatermark = Watermark.NothingValue;

    /// <summary>
    ///     Records that could not be written to the output producer (it was not accepting them). A non-empty
    ///     queue means the loop must drain it first (a fresh watermark is obtained at write time) before
    ///     consuming the next records. Watermarks are not stored on failure.
    /// </summary>
    private readonly Deque<ConsumeResult<TKey, TValue>> _pending;

    /// <summary>The idling progress-marker state machine.</summary>
    private enum IdleWatermarkState : byte
    {
        /// <summary>Consumption is progressing (records flowed since the last idle cycle).</summary>
        Default,

        /// <summary>One full waiting cycle elapsed with no data — awaiting the next wake to emit the marker.</summary>
        Prepare,

        /// <summary>The bare progress marker has been emitted for this quiet period.</summary>
        Realize
    }

    /// <summary>
    ///     The most recent watermark that was actually emitted to the output producer (either as a record
    ///     item or as a bare progress marker). A bare marker is only emitted for a new watermark that is
    ///     strictly newer than this value — a guard against pushing a stale (or repeated) progress point.
    /// </summary>
    private long _lastEmittedWatermark = Watermark.NothingValue;

    /// <summary>
    ///     The idle progress marker state machine (see <see cref="IdleWatermarkState" />). It tracks whether a
    ///     bare progress marker should be emitted now that consumption has been idle for a full waiting cycle.
    /// </summary>
    private IdleWatermarkState _idleWatermarkState;

    /// <summary>The shared loop signal multiplexer (advance/watermark timers).</summary>
    private readonly FanInSlim _fan;

    /// <summary>The offset advance strategy (OffsetStore one-by-one / ManualCommit in bulk).</summary>
    private readonly KafkaAdvanceStrategy _advanceStrategy;

    /// <summary>The one-shot advance timer (window commit / leaving emergency-idle).</summary>
    private readonly ITimer _advanceTimer;

    /// <summary>The periodic watermark timer (cutoffs: close window + flush).</summary>
    private readonly ITimer _watermarkTimer;

    private readonly KafkaErrorPolicy _errorPolicy;

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
    ///     Starts pushing watermarked results into a synchronous producer, launching the background loop
    ///     on the thread pool.
    /// </summary>
    /// <param name="target">The synchronous producer receiving the emitted items.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    /// <returns>The background consume task.</returns>
    [PublicAPI]
    public void Fuse(IProducator<Watermarked<Unio<ConsumeResult<TKey, TValue>, Unit>>> target, FlowContext context)
    {
        _ = context.RegisterBackground(() => InternalExecuteAsync(target, context));
        
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
    ///         The value is treated as a threshold: it only promotes the commit point when it strictly
    ///         exceeds the current <see cref="_thresholdWatermark" />. On promotion the previous threshold
    ///         becomes the confirmed commit point (<see cref="_readerWatermark" />) and the new value becomes
    ///         the next threshold. Repeated values (same clock tick) never advance the commit point.
    ///     </para>
    /// </remarks>
    [PublicAPI]
    public void SetReaderWatermark(Watermark watermark)
    {
        // Nothing carries no progress — ignore it.
        if (watermark.IsNothing)
            return;

        var current = (long)watermark;
        var threshold = Volatile.Read(ref _thresholdWatermark);

        // A repeated (or lower) watermark does not break the threshold — it carries no new progress.
        if (current <= threshold)
            return;

        // Break the threshold: promote it to the new value, keeping the old threshold as the outcome.
        var preview = InterlockedMath.AdvanceMax(ref _thresholdWatermark, current, threshold);

        // The promoted (old) threshold is now the confirmed commit point.
        InterlockedMath.AdvanceMax(ref _readerWatermark, preview);
    }

    /// <summary>Returns the current reader watermark (for tests/diagnostics).</summary>
    [PublicAPI]
    public Watermark GetReaderWatermark() => Watermark.From(Volatile.Read(ref _readerWatermark));

    /// <summary>
    ///     Flushes the closed head windows whose watermark has been reached by the reader's confirmed
    ///     commit point (<see cref="_readerWatermark" />).
    /// </summary>
    /// <remarks>
    ///     Reads the commit point via <see cref="Volatile" /> and advances through the window pool head.
    ///     Walks the pool from the head: for each closed window, if the reader's commit point reaches the
    ///     window watermark, the window is flushed and its slot is released (reused). If a window is not yet
    ///     confirmed, the pass stops — no further windows are flushed.
    /// </remarks>
    private void FlushReadyWindows()
    {
        var readerWatermark = Watermark.From(Volatile.Read(ref _readerWatermark));

        while (Volatile.Read(ref _closed) > 0 && _windows.PeekFirst(out var headIndex))
        {
            ref var head = ref _windows[headIndex];

            if (readerWatermark < head.Watermark)
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
    /// <param name="buffer">The write target (producer) receiving the emitted items.</param>
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
    private bool Advance(IProducator<Watermarked<Unio<ConsumeResult<TKey, TValue>, Unit>>> buffer)
    {
        ref var window = ref TakeWindow();

        // Try to drain the pending queue into the output producer.
        while (_pending.TryPeekFirst(out var result))
        {
            var watermark = _watermarkProvider.GetWatermark();

            if (!TryWriteRecord(buffer, result, watermark))
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

            if (!TryWriteRecord(buffer, result, watermark))
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
    ///     Attempts to write an idle progress marker (<see cref="Unit" />) once a waiting cycle has elapsed
    ///     with no data, so the reader watermark can advance and commit a quieted tail of offsets. This is
    ///     driven by the three-state <see cref="IdleWatermarkState" /> machine.
    /// </summary>
    /// <param name="buffer">The output producer.</param>
    /// <returns>
    ///     <see langword="true" /> when a bare progress marker was emitted (or the state advanced toward one);
    ///     <see langword="false" /> when the marker remains pending (the output was full or the new watermark
    ///     was not newer than the last emitted one).
    /// </returns>
    /// <remarks>
    ///     The marker is never recorded into a window and never buffered in <see cref="_pending" /> — it is
    ///     idempotent and, on a full output, the loop simply retries on the next idle wake.
    /// </remarks>
    private bool TryEmitIdleWatermark(IProducator<Watermarked<Unio<ConsumeResult<TKey, TValue>, Unit>>> buffer)
    {
        switch (_idleWatermarkState)
        {
            case IdleWatermarkState.Default:
                // A full waiting cycle has elapsed without a record — arm the next cycle to emit the marker.
                _idleWatermarkState = IdleWatermarkState.Prepare;
                return true;

            case IdleWatermarkState.Prepare:
            {
                var watermark = _watermarkProvider.GetWatermark();

                // Guard: never emit a marker that is not strictly newer than the last emitted progress —
                // practically always true after a wait, but protects from a stale/repeated tick.
                var last = Volatile.Read(ref _lastEmittedWatermark);
                if (watermark <= last)
                    return false;

                if (!TryWriteMarker(buffer, watermark))
                    return false;

                // The marker was accepted — the progress point was delivered once; stay quiet until new data.
                ReconcileEmittedWatermark(watermark);
                _idleWatermarkState = IdleWatermarkState.Realize;
                return true;
            }

            case IdleWatermarkState.Realize:
            default:
                // Already emitted a marker for this quiet period — nothing further to do.
                return false;
        }
    }

    /// <summary>Resets the idle progress marker state when a record has been successfully emitted.</summary>
    private void EnterDefaultOnRecord()
    {
        _idleWatermarkState = IdleWatermarkState.Default;
    }

    /// <summary>Builds and writes a watermarked item carrying a consumed record (payload branch T0).</summary>
    private bool TryWriteRecord(
        IProducator<Watermarked<Unio<ConsumeResult<TKey, TValue>, Unit>>> buffer,
        ConsumeResult<TKey, TValue> result,
        Watermark watermark)
    {
        if (!buffer.TryWrite(new Watermarked<Unio<ConsumeResult<TKey, TValue>, Unit>>(result, watermark)))
            return false;

        ReconcileEmittedWatermark(watermark);
        EnterDefaultOnRecord();
        return true;
    }

    /// <summary>Builds and writes a watermarked bare progress marker (payload branch T1, <see cref="Unit" />).</summary>
    private bool TryWriteMarker(
        IProducator<Watermarked<Unio<ConsumeResult<TKey, TValue>, Unit>>> buffer,
        Watermark watermark)
    {
        return buffer.TryWrite(new Watermarked<Unio<ConsumeResult<TKey, TValue>, Unit>>(default(Unit), watermark));
    }

    /// <summary>Records <paramref name="watermark" /> as the newest emitted progress point.</summary>
    private void ReconcileEmittedWatermark(Watermark watermark)
    {
        InterlockedMath.AdvanceMax(ref _lastEmittedWatermark, watermark);
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
