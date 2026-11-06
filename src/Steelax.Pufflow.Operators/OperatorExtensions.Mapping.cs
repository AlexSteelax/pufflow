#pragma warning disable CS1591
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


    #region producator
    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Fuse<IAsyncProducator<TSource>> left)
    {
        public Fuse<IAsyncProducator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapAProdToAProd<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapSelector, selector, default));

        public Fuse<IAsyncProducator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapAProdToAProd<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapSelector, selector, args));
    }

    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Fuse<IProducator<TSource>> left)
    {
        public Fuse<IProducator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapProdToProd<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapSelector, selector, default));

        public Fuse<IProducator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapProdToProd<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapSelector, selector, args));
    }

    #endregion

    #region consumator
    /// <param name="left">The upstream pull source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Fuse<IAsyncConsumator<TSource>> left)
    {
        public Fuse<IAsyncConsumator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapAConsToACons<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapSelector, selector, default));

        public Fuse<IAsyncConsumator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapAConsToACons<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapSelector, selector, args));
    }

    /// <param name="left">The upstream pull source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Fuse<IConsumator<TSource>> left)
    {
        public Fuse<IConsumator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapConsToCons<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapSelector, selector, default));

        public Fuse<IConsumator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapConsToCons<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapSelector, selector, args));
    }

    #endregion
}