using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Draining of warmed segments for the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: extracts
///     completed segments from the warmer (head-of-line) and pushes their accumulated groups and watermark
///     markers directly into the output producer, retaining any undrained remainder when the output is full.
/// </summary>
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>The portion of a warmed segment (keys + watermark) that could not be fully drained yet.</summary>
    private PendingSegment _pending;

    /// <summary>
    ///     Drains warmed segments into the output: a retained (partially drained) segment is finished
    ///     first (one-shot), then freshly completed segments are extracted from the warmer head-of-line.
    ///     Stops when the output is full, leaving the undrained remainder in <see cref="_pending" /> for the
    ///     next call (the caller waits for output capacity, then retries).
    /// </summary>
    /// <returns>
    ///     <see cref="FlowResult.OutputBlocked" /> when the output is full (the remainder stays in
    ///     <see cref="_pending" />); <see cref="FlowResult.Success" /> when at least one segment was drained
    ///     (the loop may retry immediately); <see cref="FlowResult.Idle" /> when there was nothing to drain.
    /// </returns>
    private FlowResult DrainWarm(IAsyncProducator<Unio<TValue, TGroup, Watermark>> writer)
    {
        var progressed = false;

        // 1. Finish any retained (partially drained) segment first; once fully drained, move straight
        //    on to draining freshly completed segments from the warmer.
        if (_pending.Keys.Count > 0)
        {
            if (!DrainSegment(ref _pending, writer))
                return FlowResult.OutputBlocked; // output full — the remainder stays in _pending

            progressed = true;
        }

        // 2. Drain freshly completed segments head-of-line.
        while (_warmer.WarmNext(_policy, out var keys, out var watermark))
        {
            var segment = new PendingSegment(new ArraySegment<TKey>(keys), watermark);

            if (!DrainSegment(ref segment, writer))
            {
                _pending = segment; // output full — retain the remainder
                return FlowResult.OutputBlocked;
            }

            progressed = true;
        }

        return progressed ? FlowResult.Success : FlowResult.Idle;
    }

    /// <summary>
    ///     Drains one warmed segment into the writer: pushes each key's groups (peek → write → advance)
    ///     and finally the covering watermark. The segment is consumed through <paramref name="segment" />:
    ///     on a full drain it is cleared, on a blocked output it is replaced with the undrained remainder
    ///     (the keys from the current position plus the watermark).
    /// </summary>
    /// <param name="segment">The segment being drained; updated in place.</param>
    /// <param name="writer">The output producer to push the drained values into.</param>
    /// <returns>
    ///     <see langword="false" /> when the output was full and the remainder was retained in
    ///     <paramref name="segment" />; otherwise <see langword="true" /> when the whole segment (including
    ///     the watermark) was drained.
    /// </returns>
    private bool DrainSegment<TWriter>(ref PendingSegment segment, TWriter writer)
        where TWriter : IAsyncProducator<Unio<TValue, TGroup, Watermark>>
    {
        var keys = segment.Keys;
        var watermark = segment.Watermark;

        for (var i = 0; i < keys.Count; i++)
        {
            if (!_delayedQueue.TryGetValue(keys[i], out var accumulator))
                continue;

            while (true)
            {
                if (!accumulator.TryPeek(out var group))
                {
                    // The key is exhausted — release its remaining weight and drop the buffer.
                    _totalWeight -= accumulator.AdvanceOrComplete();
                    _delayedQueue.Remove(keys[i]);
                    break;
                }

                if (!TryWriteOutput(writer, group))
                {
                    // Output is full — retain the rest of this segment (from this key plus the watermark).
                    segment = new PendingSegment(new ArraySegment<TKey>(keys.Array!, keys.Offset + i, keys.Count - i),
                        watermark);
                    return false;
                }

                // The group was accepted downstream — advance the accumulator.
                _totalWeight -= accumulator.AdvanceOrComplete();
            }
        }

        // All keys drained — write the covering watermark.
        if (!TryWriteOutput(writer, watermark))
        {
            // Output is full on the watermark — retain it (no keys left to drain).
            segment = new PendingSegment(ArraySegment<TKey>.Empty, watermark);
            return false;
        }

        // Fully drained — clear the segment.
        segment = default;
        return true;
    }
    
    /// <summary>
    ///     A warmed segment (a slice of extracted keys plus the covering watermark) that could not be fully
    ///     drained because the output was full; retained until it can be pushed downstream.
    /// </summary>
    internal readonly struct PendingSegment(ArraySegment<TKey> keys, Watermark watermark)
    {
        public readonly ArraySegment<TKey> Keys = keys;
        public readonly Watermark Watermark = watermark;
    }
}