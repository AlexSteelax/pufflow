using System.Diagnostics.CodeAnalysis;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>
///     A stateless 1:1 pipe that applies a projection (<see cref="MapSelector{TSource,TTarget}" />) to every element
///     it sees, forwarding each result unchanged downstream. One input always produces exactly one output.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TTarget">The output element type.</typeparam>
/// <typeparam name="TScope">
///     The type of the operator-held scope state (typically the caller-supplied selector itself), handed to the
///     internal projection on each invocation. Kept as a type parameter so the projection can be a cached static
///     method-group adapter that the JIT is able to inline on the hot path.
/// </typeparam>
/// <typeparam name="TArgs">
///     The type of the fixed per-pipe arguments the projection receives (a <see cref="Unit" /> padding when the public
///     selector takes no extra arguments).
/// </typeparam>
/// <remarks>
///     <para>
///         The transform is stateless and pushes exactly one element per input it accepts. On the push side,
///         <c>TryWrite</c> maps the value only when the downstream target is not full and immediately hands the
///         projection to it; a rejected write reports back the backpressure through <c>IsFull</c>/<c>WaitToWriteAsync</c>
///         without dropping the incoming value. On the pull side, the consumer projects each accepted element into its
///         output. No per-element buffering or background task is used, and completion is proxied to the neighbouring
///         endpoint unchanged.
///     </para>
///     <para>
///         For a watermarked input the processor keeps the marker intact: it projects the underlying value and wraps
///         the result with the source's unchanged watermark (see the watermarked mapping extensions).
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