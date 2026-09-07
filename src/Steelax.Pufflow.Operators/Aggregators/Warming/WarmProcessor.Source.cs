using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Source handling for the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: processes each
///     watermarked source value (accumulate, passthrough or warm a new key) and manages the global
///     progress watermark, which is held until all delayed data has been drained.
/// </summary>
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>The global progress watermark (max seen from the source), held until the delayed queue is empty.</summary>
    private Watermark _watermark = Watermark.Nothing();

    /// <summary>
    ///     Processes one source item: a <see cref="Carrier{T}" /> with data is a value (deduplicate key,
    ///     passthrough, accumulate or warm a new key), while an empty carrier (no data) is a pure progress point —
    ///     no value actions apply; its watermark is folded in or, when nothing is delaying output, forwarded as an
    ///     empty carrier downstream.
    /// </summary>
    /// <param name="item">The value or progress item, together with its watermark.</param>
    /// <param name="writer">The output producer to push passthrough values into.</param>
    /// <returns>
    ///     <see cref="FlowResult.Success" /> when the item was fully handled (the caller advances the cursor);
    ///     <see cref="FlowResult.OutputBlocked" />, <see cref="FlowResult.WarmerBlocked" /> or
    ///     <see cref="FlowResult.BudgetBlocked" /> when the item could not be handled yet — the caller drains
    ///     warmed data, waits for the respective signal, and retries it.
    /// </returns>
    private FlowResult TryHandleValue(scoped in Carrier<TValue> item, IAsyncProducator<Carrier<Unio<TValue, TGroup>>> writer)
    {
        var watermark = item.Watermark;

        // A pure progress point (no data): forward it as soon as nothing is delaying output, otherwise only
        // record its watermark (actions on a value do not apply).
        if (!item.HasValue)
        {
            if (_delayedQueue.Count == 0)
                return TryWriteOutput(writer, BareItem(watermark)) ? FlowResult.Success : FlowResult.OutputBlocked;

            FoldWatermark(watermark);
            return FlowResult.Success;
        }

        var value = item.Value;

        // Fold the item's watermark into the global progress watermark.
        FoldWatermark(watermark);

        var key = _keySelector.Invoke(value);

        // A key already in the delayed queue is still held (pre-warm or warmed-but-undrained):
        // accumulate — never passthrough, to preserve per-key order.
        if (_delayedQueue.TryGetValue(key, out var accumulator))
        {
            // The delayed buffers are full by weight — hold back until warmed data is drained.
            if (_totalWeight + accumulator.EstimatedWeight > _queueWeightLimit)
                return FlowResult.BudgetBlocked;

            _totalWeight += accumulator.InternalAdd(value);

            // The key is already registered in the (accepting) warmer segment — fold the new, potentially
            // hotter watermark into that segment so its release cover still reflects all its data.
            _warmer.AdvanceOpenWatermark(watermark);

            return FlowResult.Success;
        }

        if (!_policy.ShouldWarm(key))
            // Passthrough (no watermark — it is folded into the global progress watermark).
            return TryWriteOutput(writer, PassthroughItem(value)) ? FlowResult.Success : FlowResult.OutputBlocked;

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

    /// <summary>Folds <paramref name="watermark" /> into the global progress watermark when it advances it.</summary>
    private void FoldWatermark(Watermark watermark)
    {
        if (watermark > _watermark)
            _watermark = watermark;
    }

    /// <summary>
    ///     Clears the remembered global progress watermark when an emitted (drained) covering watermark equals it.
    ///     The whole region up to that value has already been flushed downstream, so there is nothing left to close
    ///     with that same value — a later end-of-stream flush must not re-emit a duplicate final marker.
    /// </summary>
    /// <param name="emitted">The watermark that has just been fully released (drained) downstream.</param>
    private void ResetGlobalAfterRelease(Watermark emitted)
    {
        if (emitted != Watermark.Nothing() && _watermark == emitted)
            _watermark = Watermark.Nothing();
    }

    /// <summary>
    ///     Emits the held global progress watermark once all delayed data has been drained. At this point
    ///     <see cref="DrainSegment" /> holds at most a pending watermark (its keys all live in the delayed
    ///     queue), which is subsumed by the global one and therefore dropped.
    /// </summary>
    /// <returns>
    ///     <see langword="false" /> when the output is full and the watermark stays held for a retry;
    ///     otherwise <see langword="true" />.
    /// </returns>
    private bool TryFlushWatermark(IAsyncProducator<Carrier<Unio<TValue, TGroup>>> writer)
    {
        if (_delayedQueue.Count > 0 || _watermark.IsNothing)
            return true;

        _pending = default;

        if (!TryWriteOutput(writer, BareItem(_watermark)))
            return false;

        _watermark = Watermark.Nothing();
        return true;
    }

    /// <summary>
    ///     The outcome of one processing step (a source value handled via <see cref="TryHandleValue" />
    ///     or warmed data drained via <see cref="DrainWarm" />); it drives the consumer loop's decision
    ///     whether to retry immediately or put an awaited source on standby.
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
        Idle
    }
}