using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>
///     A stateless 1:1 pipe that projects data flowing inside <see cref="Carrier{T}" /> envelopes. The projection
///     consumes the whole envelope (<see cref="CarrierMapSelector{TSource,TScope,TArgs,TTarget}" />) so both
///     value-bearing and bare-progress elements can be translated without an extra wrapping layer.
/// </summary>
/// <typeparam name="TSource">The input carried value element type.</typeparam>
/// <typeparam name="TTarget">The output carried value element type.</typeparam>
/// <typeparam name="TScope">The operator-held scope state handed to the projection.</typeparam>
/// <typeparam name="TArgs">The fixed per-pipe projection arguments.</typeparam>
/// <remarks>
///     Stateless: projection runs in the hotspot, backpressure and completion are proxied to the neighbouring
///     endpoint unchanged, and no buffering or background task is used.
/// </remarks>
[Flow]
internal sealed partial class BypassMapCarrierProcessor<TSource, TScope, TArgs, TTarget>(CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args)
{
    /// <summary>Exposes this pipe over an asynchronous push channel.</summary>
    /// <param name="source">The push side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream async push producer to write carriers into.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(out IAsyncProducator<Carrier<TSource>> source, IAsyncProducator<Carrier<TTarget>> target, FlowContext context)
    {
        source = new AsyncProducator(target, selector, scope, args);
    }

    /// <summary>Exposes this pipe over an asynchronous pull channel.</summary>
    /// <param name="source">The upstream async pull consumer to read carriers from.</param>
    /// <param name="target">The async consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(IAsyncConsumator<Carrier<TSource>> source, out IAsyncConsumator<Carrier<TTarget>> target, FlowContext context)
    {
        target = new AsyncConsumator(source, selector, scope, args);
    }

    /// <summary>Exposes this pipe over a push channel.</summary>
    /// <param name="source">The push side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream sync push producer to write carriers into.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(out IProducator<Carrier<TSource>> source, IProducator<Carrier<TTarget>> target, FlowContext context)
    {
        source = new Producator(target, selector, scope, args);
    }

    /// <summary>Exposes this pipe over a pull channel.</summary>
    /// <param name="source">The upstream sync pull consumer to read carriers from.</param>
    /// <param name="target">The sync consumer this component implements for the downstream.</param>
    /// <param name="context">The flow context providing cancellation.</param>
    public void Fuse(IConsumator<Carrier<TSource>> source, out IConsumator<Carrier<TTarget>> target, FlowContext context)
    {
        target = new Consumator(source, selector, scope, args);
    }

    private sealed class AsyncProducator(IAsyncProducator<Carrier<TTarget>> writer, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IAsyncProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value)
        {
            return !writer.IsFull && writer.TryWrite(selector.Invoke(value, scope, args));
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;

        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }

    private sealed class Producator(IProducator<Carrier<TTarget>> writer, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IProducator<Carrier<TSource>>
    {
        public bool TryWrite(Carrier<TSource> value)
        {
            return !writer.IsFull && writer.TryWrite(selector.Invoke(value, scope, args));
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;
    }

    private sealed class AsyncConsumator(IAsyncConsumator<Carrier<TSource>> reader, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IAsyncConsumator<Carrier<TTarget>>
    {
        public bool TryRead(out Carrier<TTarget> value)
        {
            if (reader.TryRead(out var source))
            {
                value = selector.Invoke(source, scope, args);
                return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;

        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }

    private sealed class Consumator(IConsumator<Carrier<TSource>> reader, CarrierMapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IConsumator<Carrier<TTarget>>
    {
        public bool TryRead(out Carrier<TTarget> value)
        {
            if (reader.TryRead(out var source))
            {
                value = selector.Invoke(source, scope, args);
                return true;
            }

            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
    }
}
