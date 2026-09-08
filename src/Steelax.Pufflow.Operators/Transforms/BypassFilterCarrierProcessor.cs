using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>
///     A stateless filter that inspects <see cref="Carrier{T}" /> envelopes and forwards or drops them as decided by
///     a <see cref="CarrierFilter{TSource,TScope,TArgs}" />. Because the decision runs over the envelope — not merely
///     over  the data inside it — progress-only elements can be forwarded (or suppressed) by the filter's own policy.
/// </summary>
/// <typeparam name="TSource">The carried value element type flowing through the pipe.</typeparam>
/// <typeparam name="TScope">The operator-held scope state handed to the predicate.</typeparam>
/// <typeparam name="TArgs">The fixed per-pipe predicate arguments.</typeparam>
/// <remarks>
///     Stateless: an accepted carrier is forwarded once, a rejected one (or bare progress the caller decides to
///     suppress) is dropped; backpressure and completion are proxied to the neighbouring endpoint unchanged.
/// </remarks>
[Flow]
internal sealed partial class BypassFilterCarrierProcessor<TSource, TScope, TArgs>(CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args)
{
    /// <summary>Exposes this filter over an asynchronous push channel.</summary>
    /// <param name="source">The push side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream async push producer.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(out IAsyncProducator<Carrier<TSource>> source, IAsyncProducator<Carrier<TSource>> target, FlowContext context)
    {
        source = new AsyncProducator(target, predicate, scope, args);
    }

    /// <summary>Exposes this filter over an asynchronous pull channel.</summary>
    /// <param name="source">The upstream async pull consumer.</param>
    /// <param name="target">The async consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(IAsyncConsumator<Carrier<TSource>> source, out IAsyncConsumator<Carrier<TSource>> target, FlowContext context)
    {
        target = new AsyncConsumator(source, predicate, scope, args);
    }

    /// <summary>Exposes this filter over a push channel.</summary>
    /// <param name="source">The push side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream sync push producer.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(out IProducator<Carrier<TSource>> source, IProducator<Carrier<TSource>> target, FlowContext context)
    {
        source = new Producator(target, predicate, scope, args);
    }

    /// <summary>Exposes this filter over a pull channel.</summary>
    /// <param name="source">The upstream sync pull consumer.</param>
    /// <param name="target">The sync consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(IConsumator<Carrier<TSource>> source, out IConsumator<Carrier<TSource>> target, FlowContext context)
    {
        target = new Consumator(source, predicate, scope, args);
    }

    private sealed class AsyncProducator(IAsyncProducator<Carrier<TSource>> writer, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value)
        {
            if (writer.IsFull)
                return false;
            
            return !predicate.Invoke(value, scope, args) || writer.TryWrite(value);
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;

        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }

    private sealed class Producator(IProducator<Carrier<TSource>> writer, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value)
        {
            if (writer.IsFull)
                return false;

            return !predicate.Invoke(value, scope, args) || writer.TryWrite(value);
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;
    }

    private sealed class AsyncConsumator(IAsyncConsumator<Carrier<TSource>> reader, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IAsyncConsumator<Carrier<TSource>>
    {
        public bool TryRead(out Carrier<TSource> value)
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

    private sealed class Consumator(IConsumator<Carrier<TSource>> reader, CarrierFilter<TSource, TScope, TArgs> predicate, TScope scope, TArgs args) : IConsumator<Carrier<TSource>>
    {
        public bool TryRead(out Carrier<TSource> value)
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