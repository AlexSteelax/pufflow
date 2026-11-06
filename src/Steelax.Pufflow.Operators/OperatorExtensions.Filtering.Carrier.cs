#pragma warning disable CS1591
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

    extension<TSource>(Fuse<IAsyncProducator<Carrier<TSource>>> left)
    {
        public Fuse<IAsyncProducator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
            => left.Next(BypassFilterCarrierAProdToAProd<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterCarrier, predicate, default));

        public Fuse<IAsyncProducator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
            => left.Next(BypassFilterCarrierAProdToAProd<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterCarrier, predicate, args));
    }

    extension<TSource>(Fuse<IProducator<Carrier<TSource>>> left)
    {
        public Fuse<IProducator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
            => left.Next(BypassFilterCarrierProdToProd<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterCarrier, predicate, default));

        public Fuse<IProducator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
            => left.Next(BypassFilterCarrierProdToProd<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterCarrier, predicate, args));
    }

    #endregion

    #region consumator

    extension<TSource>(Fuse<IAsyncConsumator<Carrier<TSource>>> left)
    {
        public Fuse<IAsyncConsumator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
            => left.Next(BypassFilterCarrierAConsToACons<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterCarrier, predicate, default));

        public Fuse<IAsyncConsumator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
            => left.Next(BypassFilterCarrierAConsToACons<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterCarrier, predicate, args));
    }

    extension<TSource>(Fuse<IConsumator<Carrier<TSource>>> left)
    {
        public Fuse<IConsumator<Carrier<TSource>>> Filter(FilterPredicate<TSource> predicate)
            => left.Next(BypassFilterCarrierConsToCons<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterCarrier, predicate, default));

        public Fuse<IConsumator<Carrier<TSource>>> Filter<TArgs>(FilterPredicate<TSource, TArgs> predicate, TArgs args)
            => left.Next(BypassFilterCarrierConsToCons<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterCarrier, predicate, args));
    }

    #endregion
}