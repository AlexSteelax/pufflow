namespace Steelax.Pufflow.Operators;

/// <summary>
///     Provides extension operators that attach processing stages to a dataflow source.
/// </summary>
[PublicAPI]
public static class OperatorExtensions
{
    extension<T>(Source<IAsyncEnumerator<T>> left)
    {
        /// <summary>
        ///     Groups consecutive elements into chunks of at least <paramref name="minimumSize" /> elements,
        ///     emitted when the size is reached or after <paramref name="linger" /> elapses.
        /// </summary>
        /// <param name="minimumSize">The minimum number of elements per chunk.</param>
        /// <param name="linger">The maximum time to wait for a partial chunk before emitting it.</param>
        /// <param name="strategy">The buffer-capacity strategy used to size each chunk.</param>
        /// <returns>A source emitting pooled <see cref="Chunk{T}" /> items.</returns>
        [PublicAPI]
        public Source<IAsyncEnumerator<Chunk<T>>> Chunking(int minimumSize, TimeSpan linger,
            ChunkCapacityStrategy strategy = ChunkCapacityStrategy.Exact)
        {
            return left.Next(new ChunkProcessor<T>(new Chunker<T>(strategy), minimumSize, linger));
        }

        /// <summary>
        ///     Races each upstream wait against a timeout, emitting either the element or an
        ///     <see cref="AwaitTimeout" /> marker when the source is idle too long.
        /// </summary>
        /// <param name="timeout">The maximum time to wait for an element before emitting a timeout marker.</param>
        /// <returns>A source emitting <see cref="Unio{T, AwaitTimeout}" /> items.</returns>
        [PublicAPI]
        public Source<IAsyncEnumerator<Unio<T, AwaitTimeout>>> Timeout(TimeSpan timeout)
        {
            return left.Next(new TimeoutProcessor<T>(timeout));
        }

        /// <summary>
        ///     Decouples the source from the consumer by pumping elements through a bounded in-memory buffer.
        /// </summary>
        /// <param name="capacity">The maximum number of buffered elements.</param>
        /// <returns>A source emitting the buffered elements downstream.</returns>
        [PublicAPI]
        public Source<IAsyncEnumerator<T>> Buffering(int capacity)
        {
            return left.Next(new BufferProcessor<T>(capacity));
        }
    }

    extension<TValue>(Source<IAsyncEnumerator<Watermarked<TValue>>> left)
    {
        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TGroup">The type of warmed group results produced by an accumulator.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source emitting <see cref="Unio{T,TGroup,Watermark}" /> items.</returns>
        [PublicAPI]
        public Source<IAsyncConsumator<Unio<TValue, TGroup, Watermark>>> Warming<TKey, TGroup, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            KeySelector<TValue, TKey> keySelector,
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

            return left.Next(processor.AsyncEnumeratorToAsyncConsumator);
        }

        /// <summary>
        ///     Warms the upstream stream in key segments before forwarding values downstream.
        /// </summary>
        /// <typeparam name="TKey">The key type used to partition the stream for warming.</typeparam>
        /// <typeparam name="TWarm">The warming data type produced by an <see cref="IAsyncJob{TKey,TWarm}" />.</typeparam>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <param name="jobFactory">Creates the warming jobs.</param>
        /// <param name="keySelector">Selects the warming key for each input value.</param>
        /// <param name="policy">Decides which keys require warming and receives the warm result.</param>
        /// <param name="accumulatorFactory">Creates the per-key accumulator buffers.</param>
        /// <returns>A source emitting <see cref="Unio{T,TGroup,Watermark}" /> items.</returns>
        [PublicAPI]
        public Source<IAsyncConsumator<Unio<TValue, Watermark>>> Warming<TKey, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            KeySelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue, TValue> accumulatorFactory)
            where TKey : notnull
        {
            var warmer = new Warmer<TKey, TWarm>(
                options.MaxConcurrency,
                options.MaxQueued,
                options.SegmentCapacity,
                options.SegmentLinger,
                jobFactory);

            var processor = new WarmProcessor<TKey, TValue, TValue, TWarm>(
                warmer,
                keySelector,
                policy,
                accumulatorFactory,
                options.QueueWeightLimit,
                options.WatchdogPeriod);

            return left
                .Next(processor)
                .Next(new MapProcessor<Unio<TValue, TValue, Watermark>, Unio<TValue, Watermark>>(Map));

            static Unio<TValue, Watermark> Map(Unio<TValue, TValue, Watermark> value)
            {
                if (value.TryPickT1(out var group, out var remainder))
                    return group;

                return remainder;
            }
        }
    }
}