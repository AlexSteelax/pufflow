using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Aggregators.Warming;

/// <summary>
///     Output handling for the <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}" />: pushes values directly
///     into the supplied <see cref="IAsyncProducator{T}" /> without an intermediate buffer, waiting for
///     capacity through <see cref="WarmProcessor{TKey,TValue,TGroup,TWarm}._output" /> when the producer is full.
/// </summary>
internal sealed partial class WarmProcessor<TKey, TValue, TGroup, TWarm>
{
    /// <summary>
    ///     Attempts to write one output value into the producer, applying the inline <c>Unio</c> mapping
    ///     (a passthrough value, an accumulated group or a watermark marker).
    /// </summary>
    /// <param name="writer">The output producer.</param>
    /// <param name="value">The value to write.</param>
    /// <returns>
    ///     <see langword="true" /> when the value was accepted; <see langword="false" /> when the producer is
    ///     full — the caller retains the value and waits on <see cref="_output" />.
    /// </returns>
    private bool TryWriteOutput<TWriter>(TWriter writer, Unio<TValue, TGroup, Watermark> value)
        where TWriter : IAsyncProducator<Unio<TValue, TGroup, Watermark>>
    {
        var ok = writer.TryWrite(value);
        Trace.WriteLine($"[WarmProcessor] TryWriteOutput: {(ok ? "accepted" : "BLOCKED (full)")} {value}");
        return ok;
    }

    /// <summary>
    ///     Observes the output <c>WaitToWriteAsync</c> so the loop is woken when the producer frees capacity.
    ///     Should be called once per blocked write.
    /// </summary>
    private void ArmOutputWait(IAsyncProducator<Unio<TValue, TGroup, Watermark>> writer)
    {
        _output.Observe(writer.WaitToWriteAsync());
    }

    /// <summary>
    ///     Completes the output producer after all data has been drained, propagating the optional fault.
    /// </summary>
    private void CompleteOutput(IAsyncProducator<Unio<TValue, TGroup, Watermark>> writer, Exception? ex = null)
    {
        writer.TryComplete(ex);
    }
}
