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
    ///     Attempts to write one full output item (a passthrough/group payload or a progress marker) into the
    ///     producer. A value or group item carries <see cref="Watermark.Nothing()" />; a bare <see cref="Unit" />
    ///     marker carries the real progress watermark.
    /// </summary>
    /// <param name="writer">The output producer.</param>
    /// <param name="value">The value to write.</param>
    /// <returns>
    ///     <see langword="true" /> when the value was accepted; <see langword="false" /> when the producer is
    ///     full — the caller retains the value and waits on <see cref="_output" />.
    /// </returns>
    private bool TryWriteOutput<TWriter>(TWriter writer, Watermarked<Unio<TValue, TGroup, Unit>> value)
        where TWriter : IAsyncProducator<Watermarked<Unio<TValue, TGroup, Unit>>>
    {
        var ok = writer.TryWrite(value);
        Trace.WriteLine($"[WarmProcessor] TryWriteOutput: {(ok ? "accepted" : "BLOCKED (full)")} {value}");
        return ok;
    }

    /// <summary>Builds an output item carrying a passthrough <typeparamref name="TValue" /> without a watermark.</summary>
    private static Watermarked<Unio<TValue, TGroup, Unit>> PassthroughItem(TValue value)
    {
        return new Watermarked<Unio<TValue, TGroup, Unit>>(value, Watermark.Nothing());
    }

    /// <summary>Builds an output item carrying an accumulated <typeparamref name="TGroup" /> without a watermark.</summary>
    private static Watermarked<Unio<TValue, TGroup, Unit>> GroupItem(TGroup group)
    {
        return new Watermarked<Unio<TValue, TGroup, Unit>>(group, Watermark.Nothing());
    }

    /// <summary>
    ///     Builds an output item carrying only a progress watermark: the payload is a bare <see cref="Unit" />
    ///     (no value) and the real commit point lives on the wrapping <see cref="Watermarked{T}.Watermark" />.
    /// </summary>
    private static Watermarked<Unio<TValue, TGroup, Unit>> ProgressItem(Watermark watermark)
    {
        return new Watermarked<Unio<TValue, TGroup, Unit>>(default(Unit), watermark);
    }

    /// <summary>
    ///     Observes the output <c>WaitToWriteAsync</c> so the loop is woken when the producer frees capacity.
    ///     Should be called once per blocked write.
    /// </summary>
    private void ArmOutputWait(IAsyncProducator<Watermarked<Unio<TValue, TGroup, Unit>>> writer)
    {
        _output.Observe(writer.WaitToWriteAsync());
    }

    /// <summary>
    ///     Completes the output producer after all data has been drained, propagating the optional fault.
    /// </summary>
    private void CompleteOutput(IAsyncProducator<Watermarked<Unio<TValue, TGroup, Unit>>> writer, Exception? ex = null)
    {
        writer.TryComplete(ex);
    }
}
