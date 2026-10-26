using System.Runtime.CompilerServices;
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
///         Both the pull and the push sides therefore keep an internal result queue: a consumed (or accepted)
///         input is expanded once into the queue, and the produced carriers are handed out one at a time from its
///         head. Backpressure is reported as "cannot accept the next input while the queue of the previous output
///         has not been fully drained".
///     </para>
///     <para>
///         The <see cref="ExpandSelector{TSource,TTarget}" /> returns the produced elements as a span. The pipe
///         copies them into the queue during the invocation, so the span may point to a stack frame
///         (<c>stackalloc</c>) or a reused buffer — its contents are never retained past the call.
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
    
    private struct ExpandQueue(int capacity)
    {
        private readonly Queue<TTarget> _queue = new (capacity);
        private Watermark _watermark;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(ReadOnlySpan<TTarget> items, Watermark watermark)
        {
            foreach (var item in items)
                _queue.Enqueue(item);
            
            _watermark = watermark;

            return items.Length > 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out Carrier<TTarget> carrier)
        {
            if (_queue.TryDequeue(out var item))
            {
                carrier = new Carrier<TTarget>(item, _watermark);
                return true;
            }
            
            carrier = default;
            return false;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasAny() => _queue.Count != 0;
    }
    
    private sealed class AsyncConsumator(IAsyncConsumator<Carrier<TSource>> reader, ExpandSelector<TSource, TTarget> selector) : Consumator(reader, selector), IAsyncConsumator<Carrier<TTarget>>
    {
        public ValueTask<bool> WaitToReadAsync() => Queue.HasAny() ? ValueTask.FromResult(true) : reader.WaitToReadAsync();
    }

    private class Consumator(IConsumator<Carrier<TSource>> reader, ExpandSelector<TSource, TTarget> selector) : IConsumator<Carrier<TTarget>>
    {
        protected ExpandQueue Queue = new(16);
        
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
                var expanded = selector.Invoke(carrier.Value);

                if (Queue.Enqueue(expanded, carrier.Watermark))
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