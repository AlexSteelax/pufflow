#pragma warning disable CS1591
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

    extension<TSource>(Fuse<IAsyncProducator<Carrier<TSource>>> left)
    {
        public Fuse<IAsyncProducator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapCarrierAProdToAProd<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapCarrier, selector, default));

        public Fuse<IAsyncProducator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapCarrierAProdToAProd<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapCarrier, selector, args));
    }

    extension<TSource>(Fuse<IProducator<Carrier<TSource>>> left)
    {
        public Fuse<IProducator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapCarrierProdToProd<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapCarrier, selector, default));

        public Fuse<IProducator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapCarrierProdToProd<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapCarrier, selector, args));
    }

    #endregion

    #region consumator

    extension<TSource>(Fuse<IAsyncConsumator<Carrier<TSource>>> left)
    {
        public Fuse<IAsyncConsumator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapCarrierAConsToACons<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapCarrier, selector, default));

        public Fuse<IAsyncConsumator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapCarrierAConsToACons<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapCarrier, selector, args));
    }

    extension<TSource>(Fuse<IConsumator<Carrier<TSource>>> left)
    {
        public Fuse<IConsumator<Carrier<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
            => left.Next(BypassMapCarrierConsToCons<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>.Configure(MapCarrier, selector, default));

        public Fuse<IConsumator<Carrier<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
            => left.Next(BypassMapCarrierConsToCons<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>.Configure(MapCarrier, selector, args));
    }

    #endregion
}