namespace Steelax.Pufflow.Operators;

/// <summary>
/// Source handling for the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}"/>: processes each
/// watermarked source value (accumulate, passthrough or warm a new key) and manages the global
/// progress watermark, which is held until all delayed data has been drained.
/// </summary>
public sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>
    /// The outcome of one processing step (a source value handled via <see cref="TryHandleValue"/>
    /// or warmed data drained via <see cref="DrainWarm"/>); it drives the consumer loop's decision
    /// whether to retry immediately or put an awaited source on standby.
    /// </summary>
    private enum FlowResult
    {
        /// <summary>The operation succeeded: the value was handled, or the drain made progress; the loop may retry immediately.</summary>
        Success,

        /// <summary>The output is full; wait for output capacity, then retry this value.</summary>
        OutputBlocked,

        /// <summary>The warmer has no capacity for a new warmable key; drain and wait for a warm signal, then retry.</summary>
        WarmerBlocked,

        /// <summary>The total weight of the delayed buffers reached the limit; drain warmed data to release weight, then retry.</summary>
        BudgetBlocked,

        /// <summary>No source value and no drained data; wait for any readiness signal before retrying.</summary>
        Idle,
    }

    /// <summary>The global progress watermark (max seen from the source), held until the delayed queue is empty.</summary>
    private Watermark _watermark = Watermark.Nothing();

    /// <summary>
    /// Processes one watermarked source value (already peeked from the cursor).
    /// </summary>
    /// <param name="item">The value to process, together with its watermark.</param>
    /// <returns>
    /// <see cref="FlowResult.Success"/> when the value was fully handled (the caller advances the
    /// cursor); <see cref="FlowResult.OutputBlocked"/>, <see cref="FlowResult.WarmerBlocked"/> or
    /// <see cref="FlowResult.BudgetBlocked"/> when the value could not be handled yet — the caller
    /// drains warmed data, waits for the respective signal, and retries this value.
    /// </returns>
    private FlowResult TryHandleValue(scoped in Watermarked<TValue> item)
    {
        var (value, watermark) = item;

        // Fold the value's watermark into the global progress watermark.
        if (watermark > _watermark)
            _watermark = watermark;

        var key = _keySelector.Invoke(value);

        // A key already in the delayed queue is still held (pre-warm or warmed-but-undrained):
        // accumulate — never passthrough, to preserve per-key order.
        if (_delayedQueue.TryGetValue(key, out var accumulator))
        {
            // The delayed buffers are full by weight — hold back until warmed data is drained.
            if (_totalWeight + accumulator.EstimatedWeight > _queueWeightLimit)
                return FlowResult.BudgetBlocked;

            _totalWeight += accumulator.InternalAdd(value);
            return FlowResult.Success;
        }

        if (!_policy.ShouldWarm(key))
        {
            // Passthrough (no watermark — it is folded into the global progress watermark).
            return _buffer.TryWrite(value) ? FlowResult.Success : FlowResult.OutputBlocked;
        }

        // A new warmable key: the warmer must accept it before the value is held.
        if (!_warmer.CanAdd)
            return FlowResult.WarmerBlocked;

        accumulator = _accumulatorFactory.Create(key);

        // The delayed buffers are full by weight — hold back until warmed data is drained.
        if (_totalWeight + accumulator.EstimatedWeight > _queueWeightLimit)
            return FlowResult.BudgetBlocked;

        _delayedQueue.Add(key, accumulator);

        _totalWeight += accumulator.InternalAdd(value);
        _warmer.AddKey(key, watermark);

        return FlowResult.Success;
    }

    /// <summary>
    /// Emits the held global progress watermark once all delayed data has been drained. At this point
    /// <see cref="DrainSegment"/> holds at most a pending watermark (its keys all live in the delayed
    /// queue), which is subsumed by the global one and therefore dropped.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when the output is full and the watermark stays held for a retry;
    /// otherwise <see langword="true"/>.
    /// </returns>
    private bool TryFlushWatermark()
    {
        if (_delayedQueue.Count > 0 || _watermark.IsNothing)
            return true;

        _pending = default;

        if (!_buffer.TryWrite(_watermark))
            return false;

        _watermark = Watermark.Nothing();
        return true;
    }
}
