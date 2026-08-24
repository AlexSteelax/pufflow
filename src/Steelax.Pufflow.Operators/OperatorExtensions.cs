using System.Runtime.Intrinsics;
using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Aggregators;
using Steelax.Pufflow.Operators.Aggregators.Buffering;
using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

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
    }

    extension<TValue>(Source<IAsyncProducator<Unio<TValue, Watermark>>> left)
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [PublicAPI]
        public Source<IAsyncProducator<Watermarked<TValue>>> Watermarked()
        {
            var processor = new PairValueWatermarkProcessor<TValue>();
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    extension<T>(Source<IAsyncConsumator<T>> left)
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
        public Source<IAsyncConsumator<Chunk<T>>> Chunking(int minimumSize, TimeSpan linger, ChunkCapacityStrategy strategy = ChunkCapacityStrategy.Exact)
        {
            var chunker = new Chunker<T>(strategy);
            var processor = new ChunkProcessor<T, Chunk<T>>(chunker, minimumSize, linger);
            return left.Next(processor);
        }
    }

    extension<TValue>(Source<IAsyncConsumator<Watermarked<TValue>>> left)
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
        public Source<IAsyncProducator<Unio<TValue, TGroup, Watermark>>> Warming<TKey, TGroup, TWarm>(
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
        public Source<IAsyncProducator<Unio<TValue, Watermark>>> Warming<TKey, TWarm>(
            WarmOptions options,
            IJobFactory<TKey, TWarm> jobFactory,
            MapSelector<TValue, TKey> keySelector,
            IWarmPolicy<TKey, TWarm> policy,
            IWarmAccumulatorFactory<TKey, TValue> accumulatorFactory)
            where TKey : notnull
        {
            return  left
                .Warming<TValue, TKey, TValue, TWarm>(options, jobFactory, keySelector, policy, accumulatorFactory)
                .Map(Simplify);

            Unio<TValue, Watermark> Simplify(scoped in Unio<TValue, TValue, Watermark> value)
            {
                return value.TryPickT0(out var v1, out var remainder)
                    ? v1
                    : remainder.TryPickT0(out var v2, out var watermark)
                        ? v2
                        : watermark;
            }
        }
    }

    /// <summary>
    ///     Decouples a push producer from a pull consumer over a bounded passive buffer.
    /// </summary>
    /// <typeparam name="T">The element type flowing through the buffer.</typeparam>
    /// <param name="left">The upstream push source to decouple from the downstream pull consumer.</param>
    /// <param name="capacity">The maximum number of buffered values before the producer applies backpressure.</param>
    /// <returns>A source exposing the buffered stream as a pull (consumator) interface.</returns>
    public static Source<IAsyncConsumator<T>> Buffering<T>(this Source<IProducator<T>> left, int capacity)
    {
        var processor = new BypassBufferProcessor<T>(capacity);
        return left.Next(processor.FlowProdToACons);
    }
    
    /// <summary>
    ///     Decouples a push producer from a pull consumer over a bounded passive buffer.
    /// </summary>
    /// <typeparam name="T">The element type flowing through the buffer.</typeparam>
    /// <param name="left">The upstream push source to decouple from the downstream pull consumer.</param>
    /// <param name="capacity">The maximum number of buffered values before the producer applies backpressure.</param>
    /// <returns>A source exposing the buffered stream as a pull (consumator) interface.</returns>
    public static Source<IAsyncConsumator<T>> Buffering<T>(this Source<IAsyncProducator<T>> left, int capacity)
    {
        var processor = new BypassBufferProcessor<T>(capacity);
        return left.Next(processor.FlowAProdToACons);
    }

    /// <summary>
    ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
    ///     producing a 1:1 transformed push stream.
    /// </summary>
    /// <typeparam name="TSource">The input element type.</typeparam>
    /// <typeparam name="TTarget">The output element type.</typeparam>
    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <param name="selector">The pure function applied to each element to produce the output element.</param>
    /// <returns>A source emitting the projected elements downstream.</returns>
    public static Source<IAsyncProducator<TTarget>> Map<TSource, TTarget>(this Source<IAsyncProducator<TSource>> left, MapSelector<TSource, TTarget> selector)
    {
        var processor = new BypassMapProcessor<TSource, TTarget>(selector);
        return left.Next(processor.FlowAProdToAProd);
    }
    
    /// <summary>
    ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
    ///     producing a 1:1 transformed push stream.
    /// </summary>
    /// <typeparam name="TSource">The input element type.</typeparam>
    /// <typeparam name="TTarget">The output element type.</typeparam>
    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <param name="selector">The pure function applied to each element to produce the output element.</param>
    /// <returns>A source emitting the projected elements downstream.</returns>
    public static Source<IProducator<TTarget>> Map<TSource, TTarget>(this Source<IProducator<TSource>> left, MapSelector<TSource, TTarget> selector)
    {
        var processor = new BypassMapProcessor<TSource, TTarget>(selector);
        return left.Next(processor.FlowProdToProd);
    }
}