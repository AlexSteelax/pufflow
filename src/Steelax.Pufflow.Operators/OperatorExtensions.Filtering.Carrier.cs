using System.Runtime.CompilerServices;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static bool FilterCarrier<TSource, TArgs>(scoped in Carrier<TSource> item, FilterPredicate<TSource> predicate, scoped in TArgs _)
        => !item.HasValue || predicate.Invoke(item.Value);

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private static bool FilterCarrier<TSource, TArgs>(scoped in Carrier<TSource> item, FilterPredicate<TSource, TArgs> predicate, scoped in TArgs args)
        => !item.HasValue || predicate.Invoke(item.Value, args);

    #region producator

    /// <param name="left">The asynchronous upstream producer of carriers to filter.</param>
    /// <typeparam name="TSource">The carried element type flowing through the pipe.</typeparam>
    extension<TSource>(Source<IAsyncProducator<Carrier<TSource>>> left)
    {
        /// <summary>
        ///     Keeps only the carriers whose value satisfies <paramref name="predicate" />; bare-progress elements are
        ///     structural and are always forwarded.
        /// </summary>
        /// <param name="predicate">Determines whether a carried value is kept.</param>
        /// <returns>A source carrying only the events that pass the predicate.</returns>
        public Source<IAsyncProducator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterCarrier, predicate, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>Carrier-based filter with fixed arguments over an asynchronous push source.</summary>
        public Source<IAsyncProducator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterCarrier, predicate, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    /// <param name="left">The synchronous upstream producer of carriers to filter.</param>
    /// <typeparam name="TSource">The carried element type flowing through the pipe.</typeparam>
    extension<TSource>(Source<IProducator<Carrier<TSource>>> left)
    {
        /// <summary>Carrier-based filter over a synchronous push source. See the async counterpart.</summary>
        public Source<IProducator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterCarrier, predicate, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>Carrier-based filter with fixed arguments over a synchronous push source.</summary>
        public Source<IProducator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterCarrier, predicate, args);
            return left.Next(processor.FlowProdToProd);
        }
    }

    #endregion

    #region consumator

    /// <param name="left">The asynchronous upstream consumer of carriers to filter.</param>
    /// <typeparam name="TSource">The carried element type flowing through the pipe.</typeparam>
    extension<TSource>(Source<IAsyncConsumator<Carrier<TSource>>> left)
    {
        /// <summary>Carrier-based filter over an asynchronous pull source. See the producer counterpart.</summary>
        public Source<IAsyncConsumator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterCarrier, predicate, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>Carrier-based filter with fixed arguments over an asynchronous pull source.</summary>
        public Source<IAsyncConsumator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterCarrier, predicate, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left">The synchronous upstream consumer of carriers to filter.</param>
    /// <typeparam name="TSource">The carried element type flowing through the pipe.</typeparam>
    extension<TSource>(Source<IConsumator<Carrier<TSource>>> left)
    {
        /// <summary>Carrier-based filter over a synchronous pull source. See the async counterpart.</summary>
        public Source<IConsumator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource>, Unit>(FilterCarrier, predicate, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>Carrier-based filter with fixed arguments over a synchronous pull source.</summary>
        public Source<IConsumator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
        {
            var processor = new BypassFilterCarrierProcessor<TSource, FilterPredicate<TSource, TArgs>, TArgs>(FilterCarrier, predicate, args);
            return left.Next(processor.FlowConsToCons);
        }
    }

    #endregion
}
