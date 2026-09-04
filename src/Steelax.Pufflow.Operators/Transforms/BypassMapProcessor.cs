using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>
///     A stateless push→push pipe that applies a <see cref="MapSelector{TSource,TTarget}" /> to each element
///     of an async push stream.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TTarget">The output element type.</typeparam>
/// <typeparam name="TArgs"></typeparam>
/// <typeparam name="TScope"></typeparam>
/// <remarks>
///     <para>
///         The component implements <see cref="IAsyncProducator{TSource}" /> (the input the upstream source
///         pushes into) and forwards the projected values into the downstream target through a hold-slot:
///         when the target is full, the already-projected <typeparamref name="TTarget" /> value is retained in
///         a <see cref="PendingValue{TTarget}" /> slot and pushed out first on the next write, so the selector
///         is never invoked twice for the same input and element order is preserved.
///     </para>
///     <para>
///         The transform is stateless and 1:1 (one input produces exactly one output), so the pipe needs no
///         buffering beyond the single hold-slot and no background task. Completion is proxied to the
///         downstream target unchanged.
///     </para>
/// </remarks>
[Flow]
internal sealed partial class BypassMapProcessor<TSource, TScope, TArgs, TTarget>(MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args)
{
    /// <summary>
    ///     Hands out the push input producer (this component) and captures the downstream target to write into.
    /// </summary>
    /// <param name="source">The push (producator) side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream producator to push the projected values into.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(out IAsyncProducator<TSource> source, IAsyncProducator<TTarget> target, FlowContext context)
    {
        source = new AsyncProducator(target, selector, scope, args);
    }
    
    public void Fuse(IAsyncConsumator<TSource> source, out IAsyncConsumator<TTarget> target, FlowContext context)
    {
        target = new AsyncConsumator(source, selector, scope, args);
    }
    
    /// <summary>
    ///     Hands out the push input producer (this component) and captures the downstream target to write into.
    /// </summary>
    /// <param name="source">The push (producator) side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream producator to push the projected values into.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(out IProducator<TSource> source, IProducator<TTarget> target, FlowContext context)
    {
        source = new Producator(target, selector, scope, args);
    }
    
    public void Fuse(IConsumator<TSource> source, out IConsumator<TTarget> target, FlowContext context)
    {
        target = new Consumator(source, selector, scope, args);
    }
    
    private sealed class AsyncProducator(IAsyncProducator<TTarget> writer, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) :IAsyncProducator<TSource>
    {
        public bool TryWrite(TSource value)
        {
            if (writer.IsFull)
                return false;
            
            var mapped = selector.Invoke(value, scope, args);

            return writer.TryWrite(mapped);
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;

        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }
    
    private sealed class Producator(IProducator<TTarget> writer, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IProducator<TSource>
    {
        public bool TryWrite(TSource value)
        {
            if (writer.IsFull)
                return false;
            
            var mapped = selector.Invoke(value, scope, args);

            return writer.TryWrite(mapped);
        }

        public bool TryComplete(Exception? ex = null) => writer.TryComplete(ex);

        public bool IsFull => writer.IsFull;
    }
    
    private sealed class AsyncConsumator(IAsyncConsumator<TSource> reader, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) :IAsyncConsumator<TTarget>
    {
        public bool TryRead([MaybeNullWhen(false)] out TTarget value)
        {
            if (reader.TryRead(out var original))
            {
                value = selector.Invoke(original, scope, args);
                return true;
            }
            
            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
        
        public ValueTask<bool> WaitToReadAsync() => reader.WaitToReadAsync();
    }
    
    private sealed class Consumator(IConsumator<TSource> reader, MapSelector<TSource, TScope, TArgs, TTarget> selector, TScope scope, TArgs args) : IConsumator<TTarget>
    {
        public bool TryRead([MaybeNullWhen(false)] out TTarget value)
        {
            if (reader.TryRead(out var original))
            {
                value = selector.Invoke(original, scope, args);
                return true;
            }
            
            value = default;
            return false;
        }

        public bool IsCompleted => reader.IsCompleted;
    }
}