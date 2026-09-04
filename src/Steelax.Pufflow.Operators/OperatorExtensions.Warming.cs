using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    extension<TValue>(Source<IAsyncConsumator<Watermarked<Unio<TValue, Unit>>>> left)
    {
        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream. The input item
        ///     carries a <see cref="Unio{TValue,Unit}" /> payload so a bare <see cref="Unit" /> (progress point
        ///     without data) can flow through; the output item carries a
        ///     <see cref="Unio{T,TGroup,Unit}" /> payload — a value/group or a bare progress marker with the
        ///     commit point on the wrapping <see cref="Watermarked{T}.Watermark" />.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TGroup">The type of warmed group results produced by an accumulator.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source emitting <see cref="Watermarked{T}" /> items with a <see cref="Unio{T,TGroup,Unit}" /> payload.</returns>
        [PublicAPI]
        public Source<IAsyncProducator<Watermarked<Unio<TValue, TGroup, Unit>>>> Warming<TKey, TGroup, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            MapSelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory)
            where TKey : notnull
        {
            var warmer = new Warmer<TKey, TWarm>(
                options.MaxConcurrency,
                options.MaxQueued,
                options.SegmentCapacity,
                options.SegmentLinger,
                jobFactory);

            var processor = new WarmProcessor<TKey, TValue, TGroup, TWarm>(
                warmer,
                keySelector,
                policy,
                accumulatorFactory,
                options.QueueWeightLimit,
                options.WatchdogPeriod);

            return left.Next(processor.FlowAConsToAProd);
        }

        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream, collapsing the
        ///     warmable value/group payloads into a single value slot (<typeparamref name="TValue" />) when
        ///     <c>TValue</c> also serves as the group type.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source emitting <see cref="Watermarked{T}" /> items with a <see cref="Unio{TValue,Unit}" /> payload.</returns>
        [PublicAPI]
        public Source<IAsyncProducator<Watermarked<Unio<TValue, Unit>>>> Warming<TKey, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            MapSelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue> accumulatorFactory)
            where TKey : notnull
        {
            return left
                .Warming<TValue, TKey, TValue, TWarm>(options, jobFactory, keySelector, policy, accumulatorFactory)
                .Map(Simplify);
        }
    }
    
    extension<TValue>(Source<IAsyncConsumator<Watermarked<TValue>>> left)
    {
        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream. The input item
        ///     carries a <see cref="Unio{TValue,Unit}" /> payload so a bare <see cref="Unit" /> (progress point
        ///     without data) can flow through; the output item carries a
        ///     <see cref="Unio{T,TGroup,Unit}" /> payload — a value/group or a bare progress marker with the
        ///     commit point on the wrapping <see cref="Watermarked{T}.Watermark" />.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TGroup">The type of warmed group results produced by an accumulator.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source emitting <see cref="Watermarked{T}" /> items with a <see cref="Unio{T,TGroup,Unit}" /> payload.</returns>
        [PublicAPI]
        public Source<IAsyncProducator<Watermarked<Unio<TValue, TGroup, Unit>>>> Warming<TKey, TGroup, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            MapSelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue, TGroup> accumulatorFactory)
            where TKey : notnull
        {
            var warmer = new Warmer<TKey, TWarm>(
                options.MaxConcurrency,
                options.MaxQueued,
                options.SegmentCapacity,
                options.SegmentLinger,
                jobFactory);

            var processor = new WarmProcessor<TKey, TValue, TGroup, TWarm>(
                warmer,
                keySelector,
                policy,
                accumulatorFactory,
                options.QueueWeightLimit,
                options.WatchdogPeriod);
            
            return left.Map<TValue, Unio<TValue, Unit>>(Extend).Next(processor.FlowAConsToAProd);
        }

        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream, collapsing the
        ///     warmable value/group payloads into a single value slot (<typeparamref name="TValue" />) when
        ///     <c>TValue</c> also serves as the group type.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source emitting <see cref="Watermarked{T}" /> items with a <see cref="Unio{TValue,Unit}" /> payload.</returns>
        [PublicAPI]
        public Source<IAsyncProducator<Watermarked<Unio<TValue, Unit>>>> Warming<TKey, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            MapSelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue> accumulatorFactory)
            where TKey : notnull
        {
            return left
                .Warming<TValue, TKey, TValue, TWarm>(options, jobFactory, keySelector, policy, accumulatorFactory)
                .Map(Simplify);
        }
    }
    
    private static Unio<TValue, Unit> Simplify<TValue>(scoped in Unio<TValue, TValue, Unit> item)
    {
        if (item.TryPickT2(out _, out var remainder))
            return default(Unit);

        return remainder.TryPickT0(out var v1, out var v2) ? v1 : v2;
    }
    
    private static Unio<TValue, Unit> Extend<TValue>(scoped in TValue item)
    {
        return item;
    }
}