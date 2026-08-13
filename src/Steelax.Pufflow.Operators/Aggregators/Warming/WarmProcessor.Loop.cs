using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    private async Task InternalExecuteAsync(IAsyncConsumator<Watermarked<TValue>> reader, IAsyncProducator<Unio<TValue, TGroup, Watermark>> writer, FlowContext context)
    {
        // FanInSlim does not accept a CancellationToken: on cancellation we signal a dedicated slot to
        // wake the loop sleeping on _fanIn.WaitAsync(). The loop observes the token and exits.
        await using var cancellation = context.Token.Register(() => _fanIn.Signal(CancellationSlot));

        // The periodic watchdog wakes the sleeping loop so it re-checks the state — a safety net against
        // a missed readiness signal. It lives for the duration of the loop (disposed in the finally block).
        await using var watchdog = CreateWatchdog();

        var sourceCompleted = false;

        try
        {
            while (!context.Token.IsCancellationRequested)
            {
                Trace.WriteLine($"[WarmProcessor] Loop iteration: pendingInput={_pendingInput.Occupied}, sourceCompleted={sourceCompleted}, delayed={_delayedQueue.Count}");

                // 1. Drain warmed segments (frees output capacity, delayed weight and pumps the warmer).
                var drain = DrainWarm(writer);

                // 2. Handle the current source value, unless the source has already completed.
                var result = FlowResult.Idle;
                
                if (!sourceCompleted)
                {
                    if (TryPeekSource(reader, out var item))
                    {
                        result = TryHandleValue(in item, writer);

                        if (result == FlowResult.Success)
                            AdvanceSource(reader); // the value was fully handled — move to the next
                    }
                    else if (IsCompletedSource)
                    {
                        // End of source: seal the tail segment and start pending jobs. From now on the loop
                        // only drains until the delayed queue and the progress watermark are fully emitted.
                        sourceCompleted = true;
                    }
                    // Nothing — the source is not ready yet; fall through to Idle.
                }

                // Once the source has completed, seal the tail segment and assign jobs on every iteration:
                // if all warmer slots were busy at the first Flush, the partial tail stays unassigned, and
                // AssignNextJob(forceSeal:false) from WarmNext will not seal it — otherwise the last segment
                // would never start and the loop would hang.
                if (sourceCompleted)
                    _warmer.Flush();

                // 3. Emit the held progress watermark once all delayed data has been drained.
                if (sourceCompleted && !TryFlushWatermark(writer))
                    drain = FlowResult.OutputBlocked;

                // 4. Completion: everything has been drained and emitted.
                if (sourceCompleted && _warmer.IsEmpty && _delayedQueue.Count == 0) break;

                // 5. Combine and decide: retry immediately or plan waits and sleep on the fan-in.
                var combined = result;
                if (drain == FlowResult.OutputBlocked)
                    combined = FlowResult.OutputBlocked;
                else if (drain == FlowResult.Success)
                    combined = FlowResult.Success;

                if (PrepareWait(combined, writer))
                    continue;
                
                await _fanIn.WaitAsync();
                _fanIn.Take();
            }
        }
        finally
        {
            // Always complete the output producer — on normal completion, cancellation and exceptions alike.
            // Otherwise the external reader would hang, never receiving the end-of-stream signal.
            CompleteOutput(writer);
        }
    }
}
