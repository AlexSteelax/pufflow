using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    #region consumator

    /// <typeparam name="TSource">The carried input element type.</typeparam>
    extension<TSource>(Source<IAsyncConsumator<Carrier<TSource>>> left)
    {
        /// <summary>Carrier-based expand over an asynchronous pull source. See the producer counterparts.</summary>
        [PublicAPI]
        public Source<IAsyncConsumator<Carrier<TTarget>>> Expand<TTarget>(ExpandSelector<TSource, TTarget> selector)
        {
            var processor = new BypassExpandCarrierProcessor<TSource, TTarget>(selector);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <typeparam name="TSource">The carried input element type.</typeparam>
    extension<TSource>(Source<IConsumator<Carrier<TSource>>> left)
    {
        /// <summary>Carrier-based expand over a synchronous pull source. See the producer counterparts.</summary>
        [PublicAPI]
        public Source<IConsumator<Carrier<TTarget>>> Expand<TTarget>(ExpandSelector<TSource, TTarget> selector)
        {
            var processor = new BypassExpandCarrierProcessor<TSource, TTarget>(selector);
            return left.Next(processor.FlowConsToCons);
        }
    }

    #endregion
}
