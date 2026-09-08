using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Ready-data draining for the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: a state machine that
///     extracts completed segments from the warmer (head-of-line) and emits their groups and covering watermark,
///     giving the output to the delayed buffers over the source when both compete. Intended to drive a dedicated
///     drain loop; not wired into the consumer loop yet.
/// </summary>
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>The portion of a warmed segment (keys + watermark) that could not be fully drained yet.</summary>
    private PendingSegment _pending = new(default, Watermark.Nothing());
    
    /// <summary>
    ///     Drains at most one ready batch: finishes any retained remainder, then extracts the next completed
    ///     segment from the warmer and emits its groups and covering watermark. Never touches the output when it
    ///     is full; the undrained remainder stays in <c>_pending</c> for a retry.
    /// </summary>
    /// <param name="last">
    ///     The previous step's result; used only to signal a resumed pass after backpressure (state is tracked
    ///     by <c>_pending</c> / <c>_writer.IsFull</c>). Defaults to <see cref="DrainReadyResult.None" />.
    /// </param>
    /// <returns>
    ///     <see cref="DrainReadyResult.Emitted" /> when ready data was emitted, <see cref="DrainReadyResult.None" />
    ///     when there was nothing ready, <see cref="DrainReadyResult.Backpressure" /> when the output is full.
    /// </returns>
    private DrainReadyResult DrainReadyOnce(DrainReadyResult last = DrainReadyResult.None)
    {
        // Output full — avoid touching the warmer or the delayed buffers entirely; the remainder stays put.
        if (last == DrainReadyResult.Backpressure && _writer.IsFull)
            return DrainReadyResult.Backpressure;

        if (_pending.IsNothing)
        {
            if (!_warmer.WarmNext(_policy, WarmMode.Normal, out var keys, out var watermark))
                return DrainReadyResult.None;

            Trace.WriteLine($"[WarmProcessor] drained segment: keys={keys.Length}, watermark={watermark}");
            
            _pending = new PendingSegment(keys, watermark);
        }

        {
            var keys = _pending.Keys;
            var watermark = _pending.Watermark;
        
            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];

                if (!_delayedQueue.TryGetValue(key, out var accumulator))
                    continue;

                while (accumulator.TryPeek(out var group))
                {
                    var written = _writer.TryWrite(GroupItem(group));
                    Trace.WriteLine($"[WarmProcessor] write group: key={key}, group={group}, watermark=none, ok={written}");
                    if (!written)
                    {
                        Trace.WriteLine($"[WarmProcessor] drain blocked at key {key} — retaining remainder.");
                        return DrainReadyResult.Backpressure;
                    }
                    
                    _totalWeight -= accumulator.AdvanceOrComplete();
                }
                
                _totalWeight -= accumulator.AdvanceOrComplete();
                _delayedQueue.Remove(key);
                _pending = new PendingSegment(keys[(i + 1)..], watermark);
            }

            if (!watermark.IsNothing)
            {
                Debug.Assert(_pending.Keys.Count == 0);

                var written = _writer.TryWrite(BareItem(watermark));
                Trace.WriteLine($"[WarmProcessor] write segment watermark: watermark={watermark}, ok={written}");
                if (!written)
                {
                    Trace.WriteLine("[WarmProcessor] drain blocked on the segment watermark.");
                    return DrainReadyResult.Backpressure;
                }

                ResetGlobalAfterRelease(watermark);
            
                _pending = new PendingSegment(default, Watermark.Nothing());
            }
        }

        if (_delayedQueue.Count == 0 && !_watermark.IsNothing)
        {
            var written = _writer.TryWrite(BareItem(_watermark));
            Trace.WriteLine($"[WarmProcessor] write final progress: watermark={_watermark}, ok={written}");

            if (!written)
            {
                Trace.WriteLine("[WarmProcessor] drain blocked on the final progress watermark.");
                return DrainReadyResult.Backpressure;
            }
            
            _watermark = Watermark.Nothing();
        }
        
        return DrainReadyResult.Emitted;
    }

    /// <summary>The outcome of a draining step; drives the caller's drain loop.</summary>
    private enum DrainReadyResult : byte
    {
        /// <summary>No ready data to emit (no completed segment and no retained remainder).</summary>
        None = 0,

        /// <summary>The whole ready data was emitted successfully.</summary>
        Emitted = 1,

        /// <summary>The output is full — the drain was paused with a remainder retained for a retry.</summary>
        Backpressure = 2,
    }
    
    /// <summary>Builds an output carrier carrying a passthrough <typeparamref name="TValue" />.</summary>
    private static Carrier<Unio<TValue, TGroup>> PassthroughItem(TValue value, Watermark watermark)
    {
        return new Carrier<Unio<TValue, TGroup>>(value, watermark);
    }

    /// <summary>Builds an output carrier carrying an accumulated <typeparamref name="TGroup" /> (no watermark attached).</summary>
    private static Carrier<Unio<TValue, TGroup>> GroupItem(TGroup group)
    {
        return new Carrier<Unio<TValue, TGroup>>(group, Watermark.Nothing());
    }

    /// <summary>
    ///     Builds an empty output carrier that carries only a progress watermark (no value/data).
    /// </summary>
    private static Carrier<Unio<TValue, TGroup>> BareItem(Watermark watermark)
    {
        return new Carrier<Unio<TValue, TGroup>>(watermark);
    }

    /// <summary>
    ///     A warmed segment (a slice of extracted keys plus the covering watermark) that could not be fully
    ///     drained because the output was full; retained until it can be pushed downstream.
    /// </summary>
    internal readonly struct PendingSegment(ArraySegment<TKey> keys, Watermark watermark)
    {
        public readonly ArraySegment<TKey> Keys = keys;
        public readonly Watermark Watermark = watermark;
        
        public bool IsNothing => Keys.Count == 0 && Watermark.IsNothing;
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
}