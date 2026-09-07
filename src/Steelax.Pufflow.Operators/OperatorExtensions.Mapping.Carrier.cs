using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static Carrier<TTarget> MapCarrier<TSource, TArgs, TTarget>(scoped in Carrier<TSource> item, MapSelector<TSource, TTarget> selector, scoped in TArgs _)
        => item.HasValue
            ? new Carrier<TTarget>(selector.Invoke(item.Value), item.Watermark)
            : new Carrier<TTarget>(item.Watermark);

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static Carrier<TTarget> MapCarrier<TSource, TArgs, TTarget>(scoped in Carrier<TSource> item, MapSelector<TSource, TArgs, TTarget> selector, scoped in TArgs args)
        => item.HasValue
            ? new Carrier<TTarget>(selector.Invoke(item.Value, args), item.Watermark)
            : new Carrier<TTarget>(item.Watermark);

    #region producator

    /// <param name="left">The asynchronous upstream producer of carriers to project.</param>
    /// <typeparam name="TSource">The carried input element type.</typeparam>
    extension<TSource>(Source<IAsyncProducator<Carrier<TSource>>> left)
    {
        /// <summary>
        ///     Projects each carrier's data through the value-level <paramref name="selector" /> producing a
        ///     <see cref="Carrier{T}" /> stream; bare-progress carriers are forwarded unchanged.
        /// </summary>
        /// <typeparam name="TTarget">The carried output element type.</typeparam>
        /// <param name="selector">The pure function applied to each carried value.</param>
        /// <returns>A source of the projected carriers.</returns>
        public Source<IAsyncProducator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapCarrier, selector, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>
        ///     Projects each carrier's data through the value-level <paramref name="selector" /> (plus fixed
        ///     <paramref name="args" />) producing a <see cref="Carrier{T}" /> stream.
        /// </summary>
        /// <typeparam name="TArgs">The type of fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The carried output element type.</typeparam>
        /// <param name="selector">The pure function applied to each carried value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source of the projected carriers.</returns>
        public Source<IAsyncProducator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapCarrier, selector, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    /// <param name="left">The synchronous upstream producer of carriers to project.</param>
    /// <typeparam name="TSource">The carried input element type.</typeparam>
    extension<TSource>(Source<IProducator<Carrier<TSource>>> left)
    {
        /// <summary>
        ///     Projects each carrier's data through the value-level <paramref name="selector" /> producing a
        ///     <see cref="Carrier{T}" /> stream; bare-progress carriers are forwarded unchanged.
        /// </summary>
        /// <typeparam name="TTarget">The carried output element type.</typeparam>
        /// <param name="selector">The pure function applied to each carried value.</param>
        /// <returns>A source of the projected carriers.</returns>
        public Source<IProducator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapCarrier, selector, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>
        ///     Projects each carrier's data through the value-level <paramref name="selector" /> (plus fixed
        ///     <paramref name="args" />) producing a <see cref="Carrier{T}" /> stream.
        /// </summary>
        /// <typeparam name="TArgs">The type of fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The carried output element type.</typeparam>
        /// <param name="selector">The pure function applied to each carried value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source of the projected carriers.</returns>
        public Source<IProducator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapCarrier, selector, args);
            return left.Next(processor.FlowProdToProd);
        }
    }

    #endregion

    #region consumator

    /// <param name="left">The asynchronous upstream consumer of carrier elements to project.</param>
    /// <typeparam name="TSource">The carried input element type.</typeparam>
    extension<TSource>(Source<IAsyncConsumator<Carrier<TSource>>> left)
    {
        /// <summary>Carrier-based map over an asynchronous pull source. See the producer counterparts.</summary>
        public Source<IAsyncConsumator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapCarrier, selector, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>Carrier-based map with fixed arguments over an asynchronous pull source.</summary>
        public Source<IAsyncConsumator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapCarrier, selector, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left">The synchronous upstream consumer of carrier elements to project.</param>
    /// <typeparam name="TSource">The carried input element type.</typeparam>
    extension<TSource>(Source<IConsumator<Carrier<TSource>>> left)
    {
        /// <summary>Carrier-based map over a synchronous pull source. See the producer counterparts.</summary>
        public Source<IConsumator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapCarrier, selector, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>Carrier-based map with fixed arguments over a synchronous pull source.</summary>
        public Source<IConsumator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapCarrierProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapCarrier, selector, args);
            return left.Next(processor.FlowConsToCons);
        }
    }

    #endregion
}
