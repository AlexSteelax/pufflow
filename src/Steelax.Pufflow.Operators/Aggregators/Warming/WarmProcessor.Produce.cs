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
    ///     Attempts to write one full output item (a passthrough/group carrier or an empty progress carrier) into the
    ///     producer. A value or group carrier carries its watermark; an empty carrier (no value) carries the real
    ///     progress watermark.
    /// </summary>
    /// <param name="writer">The output producer.</param>
    /// <param name="value">The carrier to write.</param>
    /// <returns>
    ///     <see langword="true" /> when the carrier was accepted; <see langword="false" /> when the producer is
    ///     full — the caller retains the carrier and waits on <see cref="_output" />.
    /// </returns>
    private bool TryWriteOutput<TWriter>(TWriter writer, Carrier<Unio<TValue, TGroup>> value)
        where TWriter : IAsyncProducator<Carrier<Unio<TValue, TGroup>>>
    {
        var ok = writer.TryWrite(value);
        Trace.WriteLine($"[WarmProcessor] TryWriteOutput: {(ok ? "accepted" : "BLOCKED (full)")} (Carrier)");
        return ok;
    }

    /// <summary>Builds an output carrier carrying a passthrough <typeparamref name="TValue" /> (no watermark attached).</summary>
    private static Carrier<Unio<TValue, TGroup>> PassthroughItem(TValue value)
    {
        return new Carrier<Unio<TValue, TGroup>>(value, Watermark.Nothing());
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
    ///     Observes the output <c>WaitToWriteAsync</c> so the loop is woken when the producer frees capacity.
    ///     Should be called once per blocked write.
    /// </summary>
    private void ArmOutputWait(IAsyncProducator<Carrier<Unio<TValue, TGroup>>> writer)
    {
        _output.Observe(writer.WaitToWriteAsync());
    }

    /// <summary>
    ///     Completes the output producer after all data has been drained, propagating the optional fault.
    /// </summary>
    private void CompleteOutput(IAsyncProducator<Carrier<Unio<TValue, TGroup>>> writer, Exception? ex = null)
    {
        writer.TryComplete(ex);
    }
}
