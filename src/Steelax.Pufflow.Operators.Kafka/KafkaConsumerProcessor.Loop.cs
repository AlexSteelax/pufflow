using Confluent.Kafka;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Kafka;

internal partial class KafkaConsumerProcessor<TKey, TValue>
{
    /// <summary>
    ///     The current working mode of the loop with respect to the advance timer
    ///     (<see cref="_advanceTimer" />). Owned exclusively by <see cref="ScheduleTask" />.
    /// </summary>
    private LoopMode _loopMode;

    /// <summary>
    ///     The working mode of the loop: it decides whether the <see cref="_advanceTimer" /> is armed
    ///     (idle/emergency) or stopped (normal), so the loop wakes up at the right pace.
    /// </summary>
    private enum LoopMode : byte
    {
        /// <summary>
        ///     Normal operation: the loop consumes eagerly and the advance timer is stopped.
        /// </summary>
        Normal,

        /// <summary>
        ///     No data is available: the advance timer is armed with <see cref="KafkaConsumerOptions.IdleInterval" />
        ///     to wake the loop periodically and retry consumption.
        /// </summary>
        Idle,

        /// <summary>
        ///     The output buffer is above the backpressure threshold (or a write failed): the advance timer is
        ///     armed with <see cref="KafkaConsumerOptions.EmergencyInterval" /> to slow down polling until the
        ///     buffer drains.
        /// </summary>
        Emergency
    }

    /// <summary>
    ///     The main consume loop: non-blocking poll, window tracking, emission to the output buffer,
    ///     and flush of confirmed windows. A single loop drives both consumption and event-driven
    ///     reactions (advance/watermark timers), with the <see cref="LoopMode" /> deciding the
    ///     advance-timer pace.
    /// </summary>
    private async Task InternalExecuteAsync(FlowContext context)
    {
        var abortToken = context.Token;
        _watermarkTimer.Change(_options.WindowLifetime, _options.WindowLifetime);

        // Worker-owned delayed progress state (only touched by this loop).
        // pendingWatermark mirrors the newest published value the loop has accepted; commit is the confirmed
        // point windows are flushed against — it always lags the newest reader progress by one significant step.
        var pendingWatermark = Watermark.Nothing();
        var commitWatermark = Watermark.Nothing();

        try
        {
            while (!abortToken.IsCancellationRequested)
            {
                var fanSet = _fan.Take();

                // In the normal mode we always consume; in idle/emergency only the advance timer wakes us.
                switch (_loopMode)
                {
                    case LoopMode.Normal:
                    case LoopMode.Emergency or LoopMode.Idle when fanSet.IsSet(AdvanceTimerSignal):
                    {
                        var advanced = Advance();

                        // Re-evaluate the actual mode: a write failure is unconditional emergency; no data
                        // falls back to idle unless the buffer is still under backpressure.
                        _loopMode = IsEmergency
                            ? LoopMode.Emergency
                            : advanced
                                ? LoopMode.Normal
                                : LoopMode.Idle;

                        // Arm/stop the advance timer according to the current mode.
                        ScheduleTask();
                        break;
                    }
                }

                // The watermark timer fired a cutoff: fix offsets (close window + flush confirmed).
                if (fanSet.IsSet(WatermarkTimerSignal))
                {
                    NextWindow();

                    // Derive the delayed commit point from the single cross-thread field: only when the reader
                    // reports a strictly newer mark does the previously accepted value become confirmed and
                    // flushable; the newest value is merely adopted as the next step. Repeated values never
                    // promote a commit by themselves.
                    var latest = Watermark.From(Volatile.Read(ref _publishedWatermark));
                    if (latest > pendingWatermark)
                    {
                        commitWatermark = pendingWatermark;
                        pendingWatermark = latest;
                    }

                    FlushReadyWindows(commitWatermark);
                }

                // Normal mode keeps spinning; otherwise stand by until a timer wakes the loop.
                if (_loopMode == LoopMode.Normal)
                    continue;

                // Before standing by, push a bare progress marker whenever the fresh provider tick has advanced
                // beyond the last emitted watermark by at least WatermarkAdvanceInterval. The interval check
                // (based on the last actually emitted value) throttles the cadence, so the marker is emitted
                // periodically — not on every idle wake — while keeping the merged stream's watermark alive.
                // It is emitted here — not in the hot polling path — so idle pushes add no work while
                // consumption is progressing.
                if (!IsEmergency)
                    TryEmitIdleWatermark();

                await _fan.WaitAsync();
            }
        }
        catch (OperationCanceledException) when (abortToken.IsCancellationRequested)
        {
            // Normal cancellation — exit the loop.
        }
        catch (KafkaLoopAbortException)
        {
            // The error policy requested a clean stop (end-of-stream): the loop exits without an error,
            // and the buffer is completed normally in the finally block below.
        }
        finally
        {
            _buffer.TryComplete();
            TryCloseConsumer();
        }
    }

    /// <summary>
    ///     Applies the current loop mode by arming, re-arming or stopping the <see cref="_advanceTimer" />.
    ///     The timer only signals the fan-in (<see cref="AdvanceTimerSignal" />) — all work is done by the
    ///     loop when it wakes.
    /// </summary>
    /// <remarks>
    ///     This centralizes all advance-timer management in one place instead of scattering it through the loop.
    /// </remarks>
    private void ScheduleTask()
    {
        switch (_loopMode)
        {
            case LoopMode.Normal:
                _advanceTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                break;

            case LoopMode.Idle:
                _advanceTimer.Change(_options.IdleInterval, Timeout.InfiniteTimeSpan);
                break;

            case LoopMode.Emergency:
                _advanceTimer.Change(_options.EmergencyInterval, Timeout.InfiniteTimeSpan);
                break;

            default:
                throw new InvalidOperationException("Unsupported loop mode.");
        }
    }

    /// <summary>Attempts to close the consumer (best-effort cleanup).</summary>
    private void TryCloseConsumer()
    {
        try
        {
            _consumer.Close();
        }
        catch (KafkaException)
        {
            // Cleanup errors are best-effort: the loop outcome is already determined.
        }
    }
}
