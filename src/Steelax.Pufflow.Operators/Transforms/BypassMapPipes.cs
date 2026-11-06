using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Abstractions;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>Async push → async push map pipe.</summary>
[Flow]
internal sealed partial class BypassMapAProdToAProd<TSource, TScope, TArgs, TTarget>
{
    public BypassMapAProdToAProd(MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        out IAsyncProducator<TSource> producer, IAsyncProducator<TTarget> writer, FlowContext _)
    {
        producer = new AsyncProducator(writer, selector, scope, args);
    }

    private sealed class AsyncProducator(IAsyncProducator<TTarget> writer, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IAsyncProducator<TSource>
    {
        public bool TryWrite(TSource value) => !writer.IsFull && writer.TryWrite(selector.Invoke(value, scope, args));
        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }
}

/// <summary>Sync push → sync push map pipe.</summary>
[Flow]
internal sealed partial class BypassMapProdToProd<TSource, TScope, TArgs, TTarget>
{
    public BypassMapProdToProd(MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        out IProducator<TSource> producer, IProducator<TTarget> writer, FlowContext _)
    {
        producer = new Producator(writer, selector, scope, args);
    }

    private sealed class Producator(IProducator<TTarget> writer, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IProducator<TSource>
    {
        public bool TryWrite(TSource value) => !writer.IsFull && writer.TryWrite(selector.Invoke(value, scope, args));
        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
    }
}

/// <summary>Async pull → async pull map pipe.</summary>
[Flow]
internal sealed partial class BypassMapAConsToACons<TSource, TScope, TArgs, TTarget>
{
    public BypassMapAConsToACons(MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        IAsyncConsumator<TSource> reader, out IAsyncConsumator<TTarget> target, FlowContext _)
    {
        target = new AsyncConsumator(reader, selector, scope, args);
    }

    private sealed class AsyncConsumator(IAsyncConsumator<TSource> reader, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IAsyncConsumator<TTarget>
    {
        public bool TryRead([MaybeNullWhen(false)] out TTarget target)
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

/// <summary>Sync pull → sync pull map pipe.</summary>
[Flow]
internal sealed partial class BypassMapConsToCons<TSource, TScope, TArgs, TTarget>
{
    public BypassMapConsToCons(MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args,
        IConsumator<TSource> reader, out IConsumator<TTarget> target, FlowContext _)
    {
        target = new Consumator(reader, selector, scope, args);
    }

    private sealed class Consumator(IConsumator<TSource> reader, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IConsumator<TTarget>
    {
        public bool TryRead([MaybeNullWhen(false)] out TTarget target)
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