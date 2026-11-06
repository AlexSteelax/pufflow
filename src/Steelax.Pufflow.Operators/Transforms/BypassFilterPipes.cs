using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Abstractions;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>Async push → async push filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterAProdToAProd<TSource, TScope, TArgs>
{
    public BypassFilterAProdToAProd(FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        out IAsyncProducator<TSource> producer, IAsyncProducator<TSource> writer, FlowContext _)
    {
        producer = new AsyncProducator(writer, predicate, scope, args);
    }

    private sealed class AsyncProducator(IAsyncProducator<TSource> writer, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncProducator<TSource>
    {
        public bool TryWrite(TSource value) => !writer.IsFull && (!predicate.Invoke(value, scope, args) || writer.TryWrite(value));
        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }
}

/// <summary>Sync push → sync push filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterProdToProd<TSource, TScope, TArgs>
{
    public BypassFilterProdToProd(FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        out IProducator<TSource> producer, IProducator<TSource> writer, FlowContext _)
    {
        producer = new Producator(writer, predicate, scope, args);
    }

    private sealed class Producator(IProducator<TSource> writer, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IProducator<TSource>
    {
        public bool TryWrite(TSource value) => !writer.IsFull && (!predicate.Invoke(value, scope, args) || writer.TryWrite(value));
        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);
        public bool IsFull => writer.IsFull;
    }
}

/// <summary>Async pull → async pull filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterAConsToACons<TSource, TScope, TArgs>
{
    public BypassFilterAConsToACons(FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        IAsyncConsumator<TSource> reader, out IAsyncConsumator<TSource> target, FlowContext _)
    {
        target = new AsyncConsumator(reader, predicate, scope, args);
    }

    private sealed class AsyncConsumator(IAsyncConsumator<TSource> reader, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncConsumator<TSource>
    {
        public bool TryRead([MaybeNullWhen(false)] out TSource value)
        {
            while (reader.TryRead(out value))
            {
                if (predicate.Invoke(value, scope, args))
                    return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }
}

/// <summary>Sync pull → sync pull filter pipe.</summary>
[Flow]
internal sealed partial class BypassFilterConsToCons<TSource, TScope, TArgs>
{
    public BypassFilterConsToCons(FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args,
        IConsumator<TSource> reader, out IConsumator<TSource> target, FlowContext _)
    {
        target = new Consumator(reader, predicate, scope, args);
    }

    private sealed class Consumator(IConsumator<TSource> reader, FilterPredicate<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IConsumator<TSource>
    {
        public bool TryRead([MaybeNullWhen(false)] out TSource value)
        {
            while (reader.TryRead(out value))
            {
                if (predicate.Invoke(value, scope, args))
                    return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
    }
}