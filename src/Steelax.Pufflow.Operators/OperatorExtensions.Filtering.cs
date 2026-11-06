#pragma warning disable CS1591
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


    #region producator

    extension<TSource>(Fuse<IAsyncProducator<TSource>> left)
    {
        public Fuse<IAsyncProducator<TSource>> Filter(FilterPredicate<TSource> selector)
            => left.Next(BypassFilterAProdToAProd<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterHandler, selector, default));

        public Fuse<IAsyncProducator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
            => left.Next(BypassFilterAProdToAProd<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterHandler, selector, args));
    }

    extension<TSource>(Fuse<IProducator<TSource>> left)
    {
        public Fuse<IProducator<TSource>> Filter(FilterPredicate<TSource> selector)
            => left.Next(BypassFilterProdToProd<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterHandler, selector, default));

        public Fuse<IProducator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
            => left.Next(BypassFilterProdToProd<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterHandler, selector, args));
    }

    #endregion

    #region consumator

    extension<TSource>(Fuse<IAsyncConsumator<TSource>> left)
    {
        public Fuse<IAsyncConsumator<TSource>> Filter(FilterPredicate<TSource> selector)
            => left.Next(BypassFilterAConsToACons<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterHandler, selector, default));

        public Fuse<IAsyncConsumator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
            => left.Next(BypassFilterAConsToACons<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterHandler, selector, args));
    }

    /// <param name="left">The upstream pull source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Fuse<IConsumator<TSource>> left)
    {
        public Fuse<IConsumator<TSource>> Filter(FilterPredicate<TSource> selector)
            => left.Next(BypassFilterConsToCons<TSource, FilterPredicate<TSource>, Unit>.Configure(FilterHandler, selector, default));

        public Fuse<IConsumator<TSource>> Filter<TArgs>(FilterPredicate<TSource, TArgs> selector, TArgs args)
            => left.Next(BypassFilterConsToCons<TSource, FilterPredicate<TSource, TArgs>, TArgs>.Configure(FilterHandler, selector, args));
    }

    #endregion
}