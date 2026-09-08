using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    extension<TValue>(Source<IAsyncConsumator<Carrier<TValue>>> left)
    {
        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream. The source element is a
        ///     <see cref="Carrier{T}" />: a data element is a value to process; an empty one is a pure progress point.
        ///     The output carries either a passthrough <typeparamref name="TValue" /> or an accumulated
        ///     <typeparamref name="TGroup" /> as its (data) payload.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TGroup">The type of warmed group results produced by an accumulator.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source emitting <see cref="Carrier{T}" /> items carrying passthrough or group results.</returns>
        [PublicAPI]
        public Source<IAsyncProducator<Carrier<Unio<TValue, TGroup>>>> Warming<TKey, TGroup, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            MapSelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory)
            where TKey : notnull
        {
            var processor = new WarmProcessor<TKey, TValue, TGroup, TWarm>(
                keySelector,
                options,
                policy,
                jobFactory,
                accumulatorFactory);

            return left.Next(processor.FlowAConsToAProd);
        }

        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream, collapsing the value/group
        ///     payload into a single slot. Used when <c>TValue</c> also serves as the group type, so no union wrapper is
        ///     needed downstream.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source carrying the warmed (value = group) results.</returns>
        [PublicAPI]
        public Source<IAsyncProducator<Carrier<TValue>>> Warming<TKey, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            MapSelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue> accumulatorFactory)
            where TKey : notnull
        {
            return left
                .Warming<TValue, TKey, TValue, TWarm>(options, jobFactory, keySelector, policy, accumulatorFactory)
                .Map(Unwrap);
        }
    }

    private static T Unwrap<T>(scoped in Unio<T, T> union) => union.TryPickT0(out var a, out var b) ? a : b;
}
