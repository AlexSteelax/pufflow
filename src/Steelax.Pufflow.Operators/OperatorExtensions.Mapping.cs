using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static TTarget MapSelector<TSource, TArgs, TTarget>(scoped in TSource source, MapSelector<TSource, TTarget> scope, scoped in TArgs _) =>
        scope.Invoke(source);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static TTarget MapSelector<TSource, TArgs, TTarget>(scoped in TSource source, MapSelector<TSource, TArgs, TTarget> scope, scoped in TArgs args) =>
        scope.Invoke(source, args);

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static Watermarked<TTarget> MapSelector<TSource, TArgs, TTarget>(scoped in Watermarked<TSource> source, WatermarkedMapSelector<TSource, TTarget> scope, scoped in TArgs _) =>
        new(scope.Invoke(source.Value, source.Watermark), source.Watermark);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static Watermarked<TTarget> MapSelector<TSource, TArgs, TTarget>(scoped in Watermarked<TSource> source, WatermarkedMapSelector<TSource, TArgs, TTarget> scope, scoped in TArgs args) =>
        new(scope.Invoke(source.Value, source.Watermark, args), source.Watermark);
    
    
    #region producator
    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IAsyncProducator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IAsyncProducator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapSelector, selector, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IAsyncProducator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapSelector, selector, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    /// <param name="left">The asynchronous upstream producer whose values are mapped.</param>
    /// <typeparam name="TSource">The element produced by the upstream.</typeparam>
    extension<TSource>(Source<IAsyncProducator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Projects the underlying value of each watermarked element and preserves its watermark.
        /// </summary>
        /// <typeparam name="TTarget">The output value element type.</typeparam>
        /// <param name="selector">The projection applied to each underlying value.</param>
        /// <returns>A source emitting re-watermarked projected values.</returns>
        public Source<IAsyncProducator<Watermarked<TTarget>>> Map<TTarget>(WatermarkedMapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(MapSelector, selector, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IAsyncProducator<Watermarked<TTarget>>> Map<TArgs, TTarget>(WatermarkedMapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(MapSelector, selector, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IProducator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IProducator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapSelector, selector, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IProducator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapSelector, selector, args);
            return left.Next(processor.FlowProdToProd);
        }
    }

    /// <param name="left">The synchronous upstream producer whose values are mapped.</param>
    /// <typeparam name="TSource">The element produced by the upstream.</typeparam>
    extension<TSource>(Source<IProducator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Projects the underlying value of each watermarked element and preserves its watermark.
        /// </summary>
        /// <typeparam name="TTarget">The output value element type.</typeparam>
        /// <param name="selector">The projection applied to each underlying value.</param>
        /// <returns>A source emitting re-watermarked projected values.</returns>
        public Source<IProducator<Watermarked<TTarget>>> Map<TTarget>(WatermarkedMapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(MapSelector, selector, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IProducator<Watermarked<TTarget>>> Map<TArgs, TTarget>(WatermarkedMapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(MapSelector, selector, args);
            return left.Next(processor.FlowProdToProd);
        }
    }
    #endregion
    
    #region consumator
    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IAsyncConsumator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IAsyncConsumator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapSelector, selector, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IAsyncConsumator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapSelector, selector, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left">The asynchronous consumer whose values are mapped (pull side).</param>
    /// <typeparam name="TSource">The element emitted by the upstream consumer.</typeparam>
    extension<TSource>(Source<IAsyncConsumator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Projects the underlying value of each watermarked element and preserves its watermark.
        /// </summary>
        /// <typeparam name="TTarget">The output value element type.</typeparam>
        /// <param name="selector">The projection applied to each underlying value.</param>
        /// <returns>A source emitting re-watermarked projected values.</returns>
        public Source<IAsyncConsumator<Watermarked<TTarget>>> Map<TTarget>(WatermarkedMapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(MapSelector, selector, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IAsyncConsumator<Watermarked<TTarget>>> Map<TArgs, TTarget>(WatermarkedMapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(MapSelector, selector, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IConsumator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IConsumator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(MapSelector, selector, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IConsumator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(MapSelector, selector, args);
            return left.Next(processor.FlowConsToCons);
        }
    }

    /// <param name="left">The synchronous consumer whose values are mapped (pull side).</param>
    /// <typeparam name="TSource">The element emitted by the upstream consumer.</typeparam>
    extension<TSource>(Source<IConsumator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Projects the underlying value of each watermarked element and preserves its watermark.
        /// </summary>
        /// <typeparam name="TTarget">The output value element type.</typeparam>
        /// <param name="selector">The projection applied to each underlying value.</param>
        /// <returns>A source emitting re-watermarked projected values.</returns>
        public Source<IConsumator<Watermarked<TTarget>>> Map<TTarget>(WatermarkedMapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(MapSelector, selector, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>
        ///     Projects each element through <paramref name="selector" /> (driving a 1:1 result); for a watermarked
        ///     input the source watermark is preserved on the projected value.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The projection applied to each element/value.</param>
        /// <param name="args">The fixed per-pipe arguments for the projection.</param>
        /// <returns>A source emitting the projected elements.</returns>
        public Source<IConsumator<Watermarked<TTarget>>> Map<TArgs, TTarget>(WatermarkedMapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedMapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(MapSelector, selector, args);
            return left.Next(processor.FlowConsToCons);
        }
    }
    #endregion
}