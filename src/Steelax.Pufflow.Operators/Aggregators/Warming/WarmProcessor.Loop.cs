namespace Steelax.Pufflow.Operators.Aggregators.Warming;

internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    private async Task InternalExecuteAsync(CancellationToken cancellationToken)
    {
        // FanInSlim does not accept a CancellationToken: on cancellation we signal a dedicated slot to
        // wake the loop sleeping on _fanIn.WaitAsync(). The loop observes the token and exits.
        await using var cancellation = cancellationToken.Register(() => _fanIn.Signal(CancellationSlot));

        // Start the periodic watchdog so it periodically wakes the sleeping loop to re-check the state — a safety
        // net against a missed readiness signal. Inert (no-op) when the period is disabled.
        _watchdog.Start();

        var lastRead = ReadSourceResult.None;
        var lastDrain = DrainReadyResult.None;
        var ttlSet = false;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Pick up the signals accumulated since the last sleep.
                var fanSet = _fanIn.Take();

                Trace.WriteLine($"[WarmProcessor] iteration: sourceFull={_writer.IsFull}, pending={_pending.Keys.Count}, " +
                                $"delayed={_delayedQueue.Count}, warmerEmpty={_warmer.IsEmpty}");
                
                // 1. Drain ready data — a phase loop: keep emitting until nothing is ready (or the output
                //    blocks or the pipeline completes). Running first gives the delayed buffers priority.
                while ((lastDrain = DrainReadyOnce(lastDrain)) == DrainReadyResult.Emitted)
                {
                    if (cancellationToken.IsCancellationRequested)
                        return;
                }
                
                // The one-shot linger expired: seal the partial tail so it can be handed to a job.
                if (fanSet.IsSet(LingerSlot))
                {
                    Trace.WriteLine($"[WarmProcessor] linger fired — sealing the tail.");
                    
                    if (_warmer.IsSinglePartial)
                        _warmer.Progress(WarmMode.SealTail);
                    
                    ttlSet = false;
                }

                // 2. Read the source — a phase loop over values, but only once no partially drained segment
                //    is held (so a fresh passthrough cannot steal the output and stall the buffers).
                if (_pending.IsNothing)
                    while ((lastRead = ReadSourceOnce(lastRead)) is var state && IsSuccessfulSource(state))
                    {
                        if (cancellationToken.IsCancellationRequested)
                            return;
                    }

                if (_warmer.IsSinglePartial && !ttlSet)
                {
                    _ttl.Start(_segmentTtl);
                    ttlSet = true;
                }

                if (lastRead == ReadSourceResult.Completed && lastDrain == DrainReadyResult.None && _warmer.IsEmpty)
                {
                    Trace.WriteLine("[WarmProcessor] source completed and everything drained — finishing.");
                    return;
                }

                Trace.WriteLine($"[WarmProcessor] sleeping: lastRead={lastRead}, lastDrain={lastDrain}.");

                // 3. Stand by until a relevant signal wakes the loop (input/warm/output/watchdog/linger).
                //    The phase loops above already drained/read as much as possible, so sleeping is correct
                //    even after successful passes — a fresh signal triggers the next work burst.
                await _fanIn.WaitAsync();
            }
        }
        finally
        {
            // Stop the watchdog so it stops waking the loop once it has exited.
            _watchdog.Stop();

            Trace.WriteLine($"[WarmProcessor] loop exiting — completing the output producer. Cancellation requested is {cancellationToken.IsCancellationRequested}.");

            // Always complete the output producer — on normal completion, cancellation and exceptions alike.
            // Otherwise the external reader would hang, never receiving the end-of-stream signal.
            _writer.TryComplete();
        }
    }
}