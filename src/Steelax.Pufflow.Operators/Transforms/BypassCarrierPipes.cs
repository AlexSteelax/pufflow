using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>Async push → async push carrier map pipe.</summary>
[Flow]
internal sealed partial class BypassMapCarrierAProdToAProd<TSource, TScope, TArgs, TTarget>
{
    public BypassMapCarrierAProdToAProd(CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        out IAsyncProducator<Carrier<TSource>> producer, IAsyncProducator<Carrier<TTarget>> writer, FlowContext _)
    {
        producer = new AsyncProducator(writer, selector, scope, args);
    }

    private sealed class AsyncProducator(IAsyncProducator<Carrier<TTarget>> writer, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IAsyncProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value) => !writer.IsFull && writer.TryWrite(selector.Invoke(value, scope, args));
        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }
}

/// <summary>Sync push → sync push carrier map pipe.</summary>
[Flow]
internal sealed partial class BypassMapCarrierProdToProd<TSource, TScope, TArgs, TTarget>
{
    public BypassMapCarrierProdToProd(CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        out IProducator<Carrier<TSource>> producer, IProducator<Carrier<TTarget>> writer, FlowContext _)
    {
        producer = new Producator(writer, selector, scope, args);
    }

    private sealed class Producator(IProducator<Carrier<TTarget>> writer, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value) => !writer.IsFull && writer.TryWrite(selector.Invoke(value, scope, args));
        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
    }
}

/// <summary>Async pull → async pull carrier map pipe.</summary>
[Flow]
internal sealed partial class BypassMapCarrierAConsToACons<TSource, TScope, TArgs, TTarget>
{
    public BypassMapCarrierAConsToACons(CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        IAsyncConsumator<Carrier<TSource>> reader, out IAsyncConsumator<Carrier<TTarget>> target, FlowContext _)
    {
        target = new AsyncConsumator(reader, selector, scope, args);
    }

    private sealed class AsyncConsumator(IAsyncConsumator<Carrier<TSource>> reader, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IAsyncConsumator<Carrier<TTarget>>
    {
        public bool TryRead(out Carrier<TTarget> target)
        {
            if (reader.TryRead(out var source))
            {
                target = selector.Invoke(source, scope, args);
                return true;
            }

            target = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }
}

/// <summary>Sync pull → sync pull carrier map pipe.</summary>
[Flow]
internal sealed partial class BypassMapCarrierConsToCons<TSource, TScope, TArgs, TTarget>
{
    public BypassMapCarrierConsToCons(CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        IConsumator<Carrier<TSource>> reader, out IConsumator<Carrier<TTarget>> target, FlowContext _)
    {
        target = new Consumator(reader, selector, scope, args);
    }

    private sealed class Consumator(IConsumator<Carrier<TSource>> reader, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IConsumator<Carrier<TTarget>>
    {
        public bool TryRead(out Carrier<TTarget> target)
        {
            if (reader.TryRead(out var source))
            {
                target = selector.Invoke(source, scope, args);
                return true;
            }

            target = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
    }
}

/// <summary>Async push → async push carrier filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterCarrierAProdToAProd<TSource, TScope, TArgs>
{
    public BypassFilterCarrierAProdToAProd(CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        out IAsyncProducator<Carrier<TSource>> producer, IAsyncProducator<Carrier<TSource>> writer, FlowContext _)
    {
        producer = new AsyncProducator(writer, predicate, scope, args);
    }

    private sealed class AsyncProducator(IAsyncProducator<Carrier<TSource>> writer, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value)
        {
            if (writer.IsFull)
                return false;

            if (predicate.Invoke(value, scope, args))
                return writer.TryWrite(value);

            return !value.HasWatermark || writer.TryWrite(new Carrier<TSource>(value.Watermark));
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }
}

/// <summary>Sync push → sync push carrier filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterCarrierProdToProd<TSource, TScope, TArgs>
{
    public BypassFilterCarrierProdToProd(CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        out IProducator<Carrier<TSource>> producer, IProducator<Carrier<TSource>> writer, FlowContext _)
    {
        producer = new Producator(writer, predicate, scope, args);
    }

    private sealed class Producator(IProducator<Carrier<TSource>> writer, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value)
        {
            if (writer.IsFull)
                return false;

            if (predicate.Invoke(value, scope, args))
                return writer.TryWrite(value);

            return !value.HasWatermark || writer.TryWrite(new Carrier<TSource>(value.Watermark));
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
    }
}

/// <summary>Async pull → async pull carrier filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterCarrierAConsToACons<TSource, TScope, TArgs>
{
    public BypassFilterCarrierAConsToACons(CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        IAsyncConsumator<Carrier<TSource>> reader, out IAsyncConsumator<Carrier<TSource>> target, FlowContext _)
    {
        target = new AsyncConsumator(reader, predicate, scope, args);
    }

    private sealed class AsyncConsumator(IAsyncConsumator<Carrier<TSource>> reader, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncConsumator<Carrier<TSource>>
    {
        public bool TryRead(out Carrier<TSource> value)
        {
            while (reader.TryRead(out value))
            {
                if (predicate.Invoke(value, scope, args))
                    return true;

                if (value.HasWatermark)
                {
                    value = new Carrier<TSource>(value.Watermark);
                    return true;
                }
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }
}

/// <summary>Sync pull → sync pull carrier filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterCarrierConsToCons<TSource, TScope, TArgs>
{
    public BypassFilterCarrierConsToCons(CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        IConsumator<Carrier<TSource>> reader, out IConsumator<Carrier<TSource>> target, FlowContext _)
    {
        target = new Consumator(reader, predicate, scope, args);
    }

    private sealed class Consumator(IConsumator<Carrier<TSource>> reader, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IConsumator<Carrier<TSource>>
    {
        public bool TryRead(out Carrier<TSource> value)
        {
            while (reader.TryRead(out value))
            {
                if (predicate.Invoke(value, scope, args))
                    return true;

                if (value.HasWatermark)
                {
                    value = new Carrier<TSource>(value.Watermark);
                    return true;
                }
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
    }
}