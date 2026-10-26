using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>
///     A stateless 1:N pipe that expands data flowing inside <see cref="Carrier{T}" /> envelopes: each
///     value-bearing carrier produces zero or more carriers downstream, while bare-progress markers are
///     forwarded unchanged so the stream watermark keeps advancing. The pipe is the engine of the
///     <c>Expand()</c> transform.
/// </summary>
/// <typeparam name="TSource">The input carried value element type.</typeparam>
/// <typeparam name="TTarget">The output carried value element type.</typeparam>
/// <remarks>
///     <para>
///         Neither endpoint is a pure pass-through, because one input expands to an unknown number of outputs.
///         The pull sides therefore keep an internal <see cref="ExpandQueue{T}" /> result buffer: a consumed
///         input is expanded once into it, and the produced carriers are handed out one at a time from its head.
///     </para>
///     <para>
///         The <see cref="ExpandSelector{TSource,TTarget}" /> produces the output elements by adding them into
///         the provided buffer; the buffer is owned and reused by the operator, so nothing is retained past the
///         invocation. A value that expands into nothing still carries the stream's watermark and is re-emitted
///         as a bare-progress marker when it has one, so the watermark never stalls; a value (or a bare marker)
///         without a watermark is dropped.
///     </para>
/// </remarks>
[Flow]
internal sealed partial class BypassExpandCarrierProcessor<TSource, TTarget>
{
    private readonly ExpandSelector<TSource, TTarget> _selector;

    internal BypassExpandCarrierProcessor(ExpandSelector<TSource, TTarget> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        _selector = selector;
    }

    /// <summary>Exposes this pipe over an asynchronous pull channel.</summary>
    /// <param name="source">The upstream async pull consumer to read carriers from.</param>
    /// <param name="target">The async consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(IAsyncConsumator<Carrier<TSource>> source, out IAsyncConsumator<Carrier<TTarget>> target, FlowContext context)
    {
        target = new AsyncConsumator(source, _selector);
    }

    /// <summary>Exposes this pipe over a pull channel.</summary>
    /// <param name="source">The upstream sync pull consumer to read carriers from.</param>
    /// <param name="target">The sync consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(IConsumator<Carrier<TSource>> source, out IConsumator<Carrier<TTarget>> target, FlowContext context)
    {
        target = new Consumator(source, _selector);
    }

    private sealed class AsyncConsumator(IAsyncConsumator<Carrier<TSource>> reader, ExpandSelector<TSource, TTarget> selector) : Consumator(reader, selector), IAsyncConsumator<Carrier<TTarget>>
    {
        public ValueTask<bool> WaitToReadAsync() => Queue.HasAny() ? ValueTask.FromResult(true) : reader.WaitToReadAsync();
    }

    private class Consumator(IConsumator<Carrier<TSource>> reader, ExpandSelector<TSource, TTarget> selector) : IConsumator<Carrier<TTarget>>
    {
        protected ExpandQueue<TTarget> Queue = new(16);

        public bool TryRead(out Carrier<TTarget> value)
        {
            DequeueLabel:

            if (Queue.TryDequeue(out value))
                return true;

            ReadNextLabel:

            if (!reader.TryRead(out var carrier))
            {
                value = default;
                return false;
            }

            if (carrier.HasValue)
            {
                Queue.Begin(carrier.Watermark);
                selector.Invoke(carrier.Value, Queue);

                if (Queue.HasAny())
                    goto DequeueLabel;
            }

            if (!carrier.HasWatermark)
                goto ReadNextLabel;

            value = new Carrier<TTarget>(carrier.Watermark);
            return true;
        }

        public bool IsCompleted => !Queue.HasAny() && reader.IsCompleted;
    }
}
