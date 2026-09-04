using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static bool FilterHandler<TSource, TArgs>(scoped in TSource source, FilterPredicate<TSource> scope, scoped in TArgs _)
        => scope.Invoke(source);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static bool FilterHandler<TSource, TArgs>(scoped in TSource source, FilterPredicate<TSource, TArgs> scope, scoped in TArgs args)
        => scope.Invoke(source, args);

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static Watermarked<Unio<TSource, Unit>> FilterHandler<TSource, TArgs>(scoped in Watermarked<TSource> source, WatermarkedFilterPredicate<TSource> scope, scoped in TArgs _) =>
        scope.Invoke(source.Value, source.Watermark) ? new Watermarked<Unio<TSource, Unit>>(source.Value, source.Watermark) : new Watermarked<Unio<TSource, Unit>>((Unit)default, source.Watermark);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static Watermarked<Unio<TSource, Unit>> FilterHandler<TSource, TArgs>(scoped in Watermarked<TSource> source, WatermarkedFilterPredicate<TSource, TArgs> scope, scoped in TArgs args) =>
        scope.Invoke(source.Value, source.Watermark, args) ? new Watermarked<Unio<TSource, Unit>>(source.Value, source.Watermark) : new Watermarked<Unio<TSource, Unit>>((Unit)default, source.Watermark);

    
    #region producator

    extension<TSource>(Source<IAsyncProducator<TSource>> left)
    {
        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />. Non-matching elements are dropped;
        ///     for a watermarked source a rejected value is re-emitted as an empty marker so progress still flows.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncProducator<TSource>> Filter(FilterPredicate<TSource> selector)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterHandler, selector, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncProducator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterHandler, selector, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    /// <param name="left">The asynchronous upstream producer whose values are filtered.</param>
    /// <typeparam name="TSource">The element produced by the upstream.</typeparam>
    extension<TSource>(Source<IAsyncProducator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />. Non-matching elements are dropped;
        ///     for a watermarked source a rejected value is re-emitted as an empty marker so progress still flows.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncProducator<Watermarked<Unio<TSource, Unit>>>> Filter(WatermarkedFilterPredicate<TSource> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource>, Unit, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncProducator<Watermarked<Unio<TSource, Unit>>>> Filter<TArgs>(WatermarkedFilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource, TArgs>, TArgs, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    extension<TSource>(Source<IProducator<TSource>> left)
    {

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />. Non-matching elements are dropped;
        ///     for a watermarked source a rejected value is re-emitted as an empty marker so progress still flows.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IProducator<TSource>> Filter(FilterPredicate<TSource> selector)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterHandler, selector, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IProducator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterHandler, selector, args);
            return left.Next(processor.FlowProdToProd);
        }
    }

    /// <param name="left">The synchronous upstream producer whose values are filtered.</param>
    /// <typeparam name="TSource">The element produced by the upstream.</typeparam>
    extension<TSource>(Source<IProducator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />. Non-matching elements are dropped;
        ///     for a watermarked source a rejected value is re-emitted as an empty marker so progress still flows.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IProducator<Watermarked<Unio<TSource, Unit>>>> Filter(WatermarkedFilterPredicate<TSource> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource>, Unit, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IProducator<Watermarked<Unio<TSource, Unit>>>> Filter<TArgs>(WatermarkedFilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource, TArgs>, TArgs, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, args);
            return left.Next(processor.FlowProdToProd);
        }
    }
    #endregion
    
    #region consumator

    extension<TSource>(Source<IAsyncConsumator<TSource>> left)
    {
        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />. Non-matching elements are dropped;
        ///     for a watermarked source a rejected value is re-emitted as an empty marker so progress still flows.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncConsumator<TSource>> Filter(FilterPredicate<TSource> selector)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterHandler, selector, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncConsumator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterHandler, selector, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left">The asynchronous upstream consumer whose elements are filtered.</param>
    /// <typeparam name="TSource">The element flowing through the consumer.</typeparam>
    extension<TSource>(Source<IAsyncConsumator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />. Non-matching elements are dropped;
        ///     for a watermarked source a rejected value is re-emitted as an empty marker so progress still flows.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncConsumator<Watermarked<Unio<TSource, Unit>>>> Filter(WatermarkedFilterPredicate<TSource> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource>, Unit, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IAsyncConsumator<Watermarked<Unio<TSource, Unit>>>> Filter<TArgs>(WatermarkedFilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource, TArgs>, TArgs, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IConsumator<TSource>> left)
    {
        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />; the rest are dropped.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IConsumator<TSource>> Filter(FilterPredicate<TSource> selector)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterHandler, selector, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IConsumator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassFilterProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterHandler, selector, args);
            return left.Next(processor.FlowConsToCons);
        }
    }

    /// <param name="left">The synchronous upstream consumer whose values are filtered.</param>
    /// <typeparam name="TSource">The element produced by the upstream.</typeparam>
    extension<TSource>(Source<IConsumator<Watermarked<TSource>>> left)
    {
        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />. Non-matching elements are dropped;
        ///     for a watermarked source a rejected value is re-emitted as an empty marker so progress still flows.
        /// </summary>
        /// <param name="selector">Determines whether an element is kept.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IConsumator<Watermarked<Unio<TSource, Unit>>>> Filter(WatermarkedFilterPredicate<TSource> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource>, Unit, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>
        ///     Keeps only the elements that satisfy <paramref name="selector" />, complementing it with fixed
        ///     per-pipe <paramref name="args" />.
        /// </summary>
        /// <typeparam name="TArgs">The type of the fixed per-pipe arguments.</typeparam>
        /// <param name="selector">Determines whether an element is kept (given the fixed arguments).</param>
        /// <param name="args">The fixed per-pipe arguments for the predicate.</param>
        /// <returns>A source of the elements that pass the predicate.</returns>
        public Source<IConsumator<Watermarked<Unio<TSource, Unit>>>> Filter<TArgs>(WatermarkedFilterPredicate<TSource, TArgs> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, WatermarkedFilterPredicate<TSource, TArgs>, TArgs, Watermarked<Unio<TSource, Unit>>>(FilterHandler, selector, args);
            return left.Next(processor.FlowConsToCons);
        }
    }
    #endregion
}