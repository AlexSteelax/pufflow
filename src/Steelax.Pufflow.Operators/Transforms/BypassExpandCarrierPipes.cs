using Celerity.Collections;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>Async pull → async pull expand pipe.</summary>
[Flow]
internal sealed partial class BypassExpandCarrierAConsToACons<TSource, TTarget>
{
    public BypassExpandCarrierAConsToACons(ExpandSelector<TSource, TTarget> selector,
        IAsyncConsumator<Carrier<TSource>> reader, out IAsyncConsumator<Carrier<TTarget>> target, FlowContext _)
    {
        ArgumentNullException.ThrowIfNull(selector);
        target = new AsyncConsumator(reader, selector);
    }

    private sealed class AsyncConsumator(IAsyncConsumator<Carrier<TSource>> reader, ExpandSelector<TSource, TTarget> selector) : Consumator(reader, selector), IAsyncConsumator<Carrier<TTarget>>
    {
        public ValueTask<bool> WaitToReadAsync() => Queue.HasAny() ? ValueTask.FromResult(true) : reader.WaitToReadAsync();
    }

    private class Consumator(IConsumator<Carrier<TSource>> reader, ExpandSelector<TSource, TTarget> selector) : IConsumator<Carrier<TTarget>>
    {
        protected QueueSpan<TTarget> Queue = new(new Deque<TTarget>(16));
        private Watermark _watermark;

        public bool TryRead(out Carrier<TTarget> value)
        {
            DequeueLabel:

            if (Queue.TryDequeue(out var item))
            {
                value = new Carrier<TTarget>(item, _watermark);
                return true;
            }

            ReadNextLabel:

            if (!reader.TryRead(out var carrier))
            {
                value = default;
                return false;
            }

            if (carrier.HasValue)
            {
                _watermark = carrier.Watermark;
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

/// <summary>Sync pull → sync pull expand pipe.</summary>
[Flow]
internal sealed partial class BypassExpandCarrierConsToCons<TSource, TTarget>
{
    public BypassExpandCarrierConsToCons(ExpandSelector<TSource, TTarget> selector,
        IConsumator<Carrier<TSource>> reader, out IConsumator<Carrier<TTarget>> target, FlowContext _)
    {
        ArgumentNullException.ThrowIfNull(selector);
        target = new Consumator(reader, selector);
    }

    private class Consumator(IConsumator<Carrier<TSource>> reader, ExpandSelector<TSource, TTarget> selector) : IConsumator<Carrier<TTarget>>
    {
        protected QueueSpan<TTarget> Queue = new(new Deque<TTarget>(16));
        private Watermark _watermark;

        public bool TryRead(out Carrier<TTarget> value)
        {
            DequeueLabel:

            if (Queue.TryDequeue(out var item))
            {
                value = new Carrier<TTarget>(item, _watermark);
                return true;
            }

            ReadNextLabel:

            if (!reader.TryRead(out var carrier))
            {
                value = default;
                return false;
            }

            if (carrier.HasValue)
            {
                _watermark = carrier.Watermark;
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