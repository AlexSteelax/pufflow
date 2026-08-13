namespace Steelax.Pufflow.Operators;

/// <summary>
///     Standby decision-making for the consumer loop of <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />:
///     translates a <see cref="FlowResult" /> into a fan-in wait — the sources of readiness are already wired
///     to their slots (the warmer via <see cref="WarmSlot" />, the source enumerator via the bridge, and the
///     output buffer's capacity via <see cref="ResultSlot" />) — and reports whether the loop may retry
///     immediately or must await a signal.
/// </summary>
public sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>
    ///     Decides, from the last operation's <see cref="FlowResult" />, whether the consumer loop can retry
    ///     immediately or must stand by on the fan-in. All sources of readiness are already wired: the warmer
    ///     signals <see cref="WarmSlot" /> via <c>OnReady</c>, the source enumerator signals its slot via the
    ///     bridge, and the output buffer's capacity release is wired to <see cref="ResultSlot" />.
    /// </summary>
    /// <param name="result">The outcome of the last source/drain operation.</param>
    /// <returns>
    ///     <see langword="true" /> when the loop may retry immediately; <see langword="false" /> when the
    ///     loop must <c>await _fanIn.WaitAsync()</c> — the relevant slot is wired and will wake it.
    /// </returns>
    private bool PrepareWait(FlowResult result)
    {
        switch (result)
        {
            case FlowResult.Success:
                // There was progress — retry immediately.
                return true;

            case FlowResult.WarmerBlocked:
                // When the whole warmer queue is filled with already-completed jobs (QueueFilled),
                // no new OnReady notifications will fire until they are drained. Sleeping on WarmSlot is
                // pointless — drain right now (the next iteration will call DrainWarm).
                if (_warmer.QueueFilled)
                    return true;

                return false;

            case FlowResult.OutputBlocked:
            case FlowResult.BudgetBlocked:
            case FlowResult.Idle:
                // Output full → ResultSlot; weight → WarmSlot; otherwise any signal.
                return false;

            default:
                return true;
        }
    }
}