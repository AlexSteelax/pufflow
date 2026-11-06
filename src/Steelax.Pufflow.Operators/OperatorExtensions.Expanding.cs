#pragma warning disable CS1591
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    #region consumator

    extension<TSource>(Fuse<IAsyncConsumator<Carrier<TSource>>> left)
    {
        [PublicAPI]
        public Fuse<IAsyncConsumator<Carrier<TTarget>>> Expand<TTarget>(ExpandSelector<TSource, TTarget> selector)
            => left.Next(BypassExpandCarrierAConsToACons<TSource, TTarget>.Configure(selector));
    }

    extension<TSource>(Fuse<IConsumator<Carrier<TSource>>> left)
    {
        [PublicAPI]
        public Fuse<IConsumator<Carrier<TTarget>>> Expand<TTarget>(ExpandSelector<TSource, TTarget> selector)
            => left.Next(BypassExpandCarrierConsToCons<TSource, TTarget>.Configure(selector));
    }

    #endregion
}