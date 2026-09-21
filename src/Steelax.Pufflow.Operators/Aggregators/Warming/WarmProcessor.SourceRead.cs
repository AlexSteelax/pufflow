using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Internal;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Source reading for the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: a state machine that
///     advances the wrapped <see cref="ManualConsumator{T}" /> source one value at a time and reports the
///     outcome. Intended to drive a dedicated read loop with output-capacity priority over the delayed
///     buffers; not wired into the consumer loop yet.
/// </summary>
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>The global progress watermark (max seen from the source), held until the delayed queue is empty.</summary>
    private Watermark _watermark = Watermark.Nothing();
    
    /// <summary>
    ///     Advances the source by at most one value and reports the outcome.
    /// </summary>
    /// <param name="last">The previous step's result; defaults to <see cref="ReadSourceResult.None" />.</param>
    /// <returns>A <see cref="ReadSourceResult" /> describing what happened.</returns>
    /// <remarks>
    ///     The method keys its behavior on <paramref name="last" /> as a state machine:
    ///     <list type="bullet">
    ///         <item>
    ///             <description>
    ///                 <see cref="ReadSourceResult.None" />, <see cref="ReadSourceResult.Warming" /> and
    ///                 <see cref="ReadSourceResult.Passthrough" /> are progress states: read a fresh value.
    ///             </description>
    ///         </item>
    ///         <item>
    ///             <description>
    ///                 <see cref="ReadSourceResult.Backpressure" /> resumes by retrying the passthrough write
    ///                 (value still held in the source, not acknowledged).
    ///             </description>
    ///         </item>
    ///         <item>
    ///             <description>
    ///                 <see cref="ReadSourceResult.Quota" /> resumes by retrying the warming placement.
    ///             </description>
    ///         </item>
    ///     </list>
    /// </remarks>
    private ReadSourceResult ReadSourceOnce(ReadSourceResult last = ReadSourceResult.None)
    {
        switch (last)
        {
            case ReadSourceResult.None:
            case ReadSourceResult.Warming:
            case ReadSourceResult.Passthrough:
            case ReadSourceResult.Backpressure when !IsBackpressure:
            case ReadSourceResult.Quota when !IsQuota:
                if (!_source.TryGet(out var item))
                {
                    var noData = _source.IsCompleted ? ReadSourceResult.Completed : ReadSourceResult.None;
                    Trace.WriteLine($"[WarmProcessor] source: {(noData == ReadSourceResult.Completed ? "completed" : "no data")}.");
                    return noData;
                }
                
                var result = item.HasValue ? ProcessDataValue(item.Value, item.Watermark) : TryForwardProgress(item.Watermark);

                if (IsSuccessfulSource(result))
                    _source.Ack(); // acknowledged: the value was fully handled вЂ” move to the next

                Trace.WriteLine($"[WarmProcessor] source value → {result}.");

                return result;

            case ReadSourceResult.Backpressure:
            case ReadSourceResult.Quota:
            case ReadSourceResult.Completed:
                return last;

            default:
                throw new InvalidOperationException();
        }
    }

    /// <summary>Routes a data value: fold its watermark, then accumulate / passthrough / warm a fresh key.</summary>
    private ReadSourceResult ProcessDataValue(scoped in TValue value, Watermark watermark)
    {
        var key = _keySelector.Invoke(value);
        
        if (_delayedQueue.TryGetValue(key, out var accumulator))
            return Accumulate(accumulator, in value, watermark);

        if (_policy.ShouldWarm(key))
            return PlaceOnWarm(key, in value, watermark);

        var fold = _delayedQueue.Count != 0;
        return EmitPassthrough(value, watermark, fold);
    }

    // ------------------------------------------------------------------
    //  Building blocks
    // ------------------------------------------------------------------

    /// <summary>Accumulates a value into an already-held key's buffer; Quota if the weight budget is exceeded.</summary>
    private ReadSourceResult Accumulate(WarmAccumulator<TValue, TGroup> accumulator, scoped in TValue value, Watermark watermark)
    {
        if (_totalWeight + accumulator.EstimatedWeight > _queueWeightLimit)
            return ReadSourceResult.Quota;

        _totalWeight += accumulator.InternalAdd(value);
        FoldWatermark(watermark);
        _warmer.AdvanceOpenWatermark(watermark);

        return ReadSourceResult.Warming;
    }

    /// <summary>Places a fresh warmable key on the warming pipeline; Quota on warmer capacity or weight budget.</summary>
    private ReadSourceResult PlaceOnWarm(TKey key, scoped in TValue value, Watermark watermark)
    {
        if (!_warmer.CanAdd)
            return ReadSourceResult.Quota;

        var accumulator = _accumulatorFactory.Create(key);

        if (_totalWeight + accumulator.EstimatedWeight > _queueWeightLimit)
            return ReadSourceResult.Quota;

        _delayedQueue.Add(key, accumulator);
        _totalWeight += accumulator.InternalAdd(value);
        FoldWatermark(watermark);

        _warmer.AddKey(key, watermark);

        return ReadSourceResult.Warming;
    }

    /// <summary>Emits a passthrough value; Backpressure when the output is full.</summary>
    private ReadSourceResult EmitPassthrough(scoped in TValue value, Watermark watermark, bool fold)
    {
        if (fold)
        {
            FoldWatermark(watermark);
            watermark =  Watermark.Nothing();
        }
        
        var item = PassthroughItem(value, watermark);
        var written = _writer.TryWrite(item);

        Trace.WriteLine($"[WarmProcessor] write passthrough: value={value}, watermark={(watermark.IsNothing ? "none" : watermark.ToString())}, ok={written}");

        return written ? ReadSourceResult.Passthrough : ReadSourceResult.Backpressure;
    }

    /// <summary>Emits (or folds) a pure progress watermark; Backpressure when the output is full.</summary>
    private ReadSourceResult TryForwardProgress(Watermark watermark)
    {
        if (watermark.IsNothing)
            return ReadSourceResult.Passthrough;
        
        if (_delayedQueue.Count == 0)
        {
            var written = _writer.TryWrite(BareItem(watermark));
            Trace.WriteLine($"[WarmProcessor] write bare progress: watermark={watermark}, ok={written}");
            return written ? ReadSourceResult.Passthrough : ReadSourceResult.Backpressure;
        }

        FoldWatermark(watermark);
        return ReadSourceResult.Passthrough;
    }
    
    private static bool IsSuccessfulSource(ReadSourceResult state) => state is ReadSourceResult.Passthrough or ReadSourceResult.Warming;
    private bool IsQuota => _totalWeight >= _queueWeightLimit;
    private bool IsBackpressure => _writer.IsFull;
    
    /// <summary>Folds <paramref name="watermark" /> into the global progress watermark when it advances it.</summary>
    private void FoldWatermark(Watermark watermark)
    {
        if (watermark > _watermark)
            _watermark = watermark;
    }

    /// <summary>The outcome of one source-reading step; drives the caller's read loop.</summary>
    private enum ReadSourceResult : byte
    {
        /// <summary>No value read (source not ready) or the default starting state.</summary>
        None = 0,

        /// <summary>The source has completed; no more values will arrive.</summary>
        Completed = 1,

        /// <summary>A passthrough value (or progress watermark) was emitted successfully.</summary>
        Passthrough = 2,

        /// <summary>A key was placed on (or is held by) the warming pipeline successfully.</summary>
        Warming = 3,

        /// <summary>Warming could not be scheduled no warmer slot / exhausted delayed-weight budget.</summary>
        Quota = 4,

        /// <summary>The output is full; the passthrough value could not be written.</summary>
        Backpressure = 5
    }
}
