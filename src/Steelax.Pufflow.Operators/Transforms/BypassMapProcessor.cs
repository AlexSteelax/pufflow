using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators.Transforms;

/// <summary>
///     A stateless push→push pipe that applies a <see cref="MapSelector{TSource,TTarget}" /> to each element
///     of an async push stream.
/// </summary>
/// <typeparam name="TSource">The input element type.</typeparam>
/// <typeparam name="TTarget">The output element type.</typeparam>
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
internal sealed partial class BypassMapProcessor<TSource, TTarget>(MapSelector<TSource, TTarget> selector)
{
    /// <summary>
    ///     Hands out the push input producer (this component) and captures the downstream target to write into.
    /// </summary>
    /// <param name="source">The push (producator) side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream producator to push the projected values into.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(out IAsyncProducator<TSource> source, IAsyncProducator<TTarget> target, FlowContext context)
    {
        source = new AsyncProducator(target, selector);
    }
    
    /// <summary>
    ///     Hands out the push input producer (this component) and captures the downstream target to write into.
    /// </summary>
    /// <param name="source">The push (producator) side this component implements; written by the upstream source.</param>
    /// <param name="target">The downstream producator to push the projected values into.</param>
    /// <param name="context">The flow context providing cancellation for the pipeline.</param>
    public void Fuse(out IProducator<TSource> source, IProducator<TTarget> target, FlowContext context)
    {
        source = new Producator(target, selector);
    }

    private sealed class AsyncProducator(IAsyncProducator<TTarget> writer, MapSelector<TSource, TTarget> selector) : IAsyncProducator<TSource>
    {
        /// <summary>The projected value retained while the downstream target is full (single-element hold-slot).</summary>
        private PendingValue<TTarget> _pending;

        /// <summary>
        ///     Accepts a source value: first flushes any retained projected value (the hold-slot) into the
        ///     target, then projects the new value. When the target is full, the projected value is retained in
        ///     the hold-slot so the selector is not re-invoked and ordering is preserved.
        /// </summary>
        /// <param name="value">The source value to project and forward.</param>
        /// <returns><see langword="true" /> when the value was accepted; otherwise <see langword="false" /> (backpressure).</returns>
        public bool TryWrite(TSource value)
        {
            if (_pending.Occupied)
            {
                if (!writer.TryWrite(_pending.Value))
                    return false;

                _pending = default;
                return true;
            }

            var mapped = selector.Invoke(value);

            if (writer.TryWrite(mapped))
                return true;

            _pending = new PendingValue<TTarget>(mapped);
            return false;
        }

        /// <summary>Signals the end of the stream on the downstream target.</summary>
        public bool TryComplete(Exception? ex = null)
        {
            return writer.TryComplete(ex);
        }

        /// <summary>Delegates the write-readiness wait to the downstream target.</summary>
        public ValueTask<bool> WaitToWriteAsync() => writer.WaitToWriteAsync();
    }
    
    private sealed class Producator(IProducator<TTarget> writer, MapSelector<TSource, TTarget> selector) : IProducator<TSource>
    {
        /// <summary>The projected value retained while the downstream target is full (single-element hold-slot).</summary>
        private PendingValue<TTarget> _pending;
        
        /// <summary>
        ///     Accepts a source value: first flushes any retained projected value (the hold-slot) into the
        ///     target, then projects the new value. When the target is full, the projected value is retained in
        ///     the hold-slot so the selector is not re-invoked and ordering is preserved.
        /// </summary>
        /// <param name="value">The source value to project and forward.</param>
        /// <returns><see langword="true" /> when the value was accepted; otherwise <see langword="false" /> (backpressure).</returns>
        public bool TryWrite(TSource value)
        {
            if (_pending.Occupied)
            {
                if (!writer.TryWrite(_pending.Value))
                    return false;
            
                _pending = default;
                return true;
            }

            var mapped = selector.Invoke(value);

            if (writer.TryWrite(mapped))
                return true;

            _pending = new PendingValue<TTarget>(mapped);
            return false;
        }

        /// <summary>Signals the end of the stream on the downstream target.</summary>
        public bool TryComplete(Exception? ex = null)
        {
            return writer.TryComplete(ex);
        }
    }
}
