using Steelax.Pufflow.Operators.Common;
using Steelax.Toolkit.HighPerformance;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Standby decision-making for the consumer loop of <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />:
///     translates a <see cref="FlowResult" /> into the readiness source to wait on — the warmer via
///     <see cref="WarmSlot" />, the input consumator via <see cref="_input" />, the output producer via
///     <see cref="_output" /> — and reports whether the loop may retry immediately or must await a signal.
/// </summary>
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>The readiness source the loop must wait on.</summary>
    private enum WaitSource
    {
        /// <summary>No wait — retry immediately (there was progress).</summary>
        None,

        /// <summary>Any signal wakes the loop (input, output, warmer, watchdog, cancellation).</summary>
        Any,

        /// <summary>The input consumator — wait until it has data.</summary>
        Input,

        /// <summary>The output producer — wait until it frees capacity.</summary>
        Output,

        /// <summary>The warmer — wait until a warm job completes.</summary>
        Warmer
    }

    /// <summary>
    ///     Decides, from the last operation's <see cref="FlowResult" />, what the consumer loop waits on.
    ///     All readiness sources are wired to the fan-in: <see cref="_input" /> → <see cref="InputSlot" />,
    ///     <see cref="_output" /> → <see cref="OutputSlot" />, the warmer → <see cref="WarmSlot" />.
    /// </summary>
    /// <param name="result">The outcome of the last source/drain operation.</param>
    /// <param name="writer"></param>
    /// <returns>The <see cref="WaitSource" /> the loop must await (or <see cref="WaitSource.None" /> to retry).</returns>
    private bool PrepareWait<TWriter>(FlowResult result, TWriter writer)
        where TWriter : IAsyncProducator<Unio<TValue, TGroup, Watermark>>
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
                return _output.Observe(writer.WaitToWriteAsync(), OnCompletedBehavior.SkipCallbackIfCompleted);
            case FlowResult.BudgetBlocked:
            case FlowResult.Idle:
                // Output full → ResultSlot; weight → WarmSlot; otherwise any signal.
                return false;

            default:
                return true;
        }
    }
}
