using Steelax.Pufflow.Operators.Aggregators.Buffering;
using Steelax.Pufflow.Operators.Aggregators.Merging;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Builders;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     Provides extension operators that attach processing stages to a dataflow source.
/// </summary>
[PublicAPI]
public static partial class OperatorExtensions
{
    extension<TValue>(Source<IAsyncConsumator<Carrier<TValue>>> left)
    {
        /// <summary>
        ///     Starts the fluent warming stage builder. The stream is warmed in key segments before values are
        ///     forwarded downstream, letting per-key background jobs (e.g. caching or enrichment) complete before
        ///     related values pass through. A carrier is a data element (a value to process) or an empty progress
        ///     point; the output carries either a passthrough value or an accumulated group as its payload.
        /// </summary>
        /// <param name="options">Numeric and timing configuration (concurrency, segments, budget, watchdog).</param>
        /// <returns>
        ///     A chain entry point: continue with <c>PartitionBy</c> → <c>Prefetch</c> → <c>Accumulate</c>.
        /// </returns>
        [PublicAPI]
        public WarmingBuilder<TValue> Warming(WarmOptions options) => new(left, options);
    }
    
    /// <summary>
    ///     Merges several pull sources into a single source: the resulting stream emits the carriers of all
    ///     inputs in an interleaved order, and its watermark advances as the minimum over the sources (using a
    ///     round-based barrier) so downstream stages never outrun the slowest input.
    /// </summary>
    /// <typeparam name="T">The element type carried by the merged sources.</typeparam>
    /// <param name="flowSource">The flow source hosting the merged stage.</param>
    /// <param name="sources">The pull inputs to merge (2..32 inputs).</param>
    /// <returns>A source emitting the merged <see cref="Carrier{T}" /> stream.</returns>
    /// <remarks>
    ///     The merged source completes when <b>every</b> input has completed; the final bare-progress carrier
    ///     carries the maximum watermark seen across the sources. Inputs can be produced by any pull stage,
    ///     e.g. <see cref="Expose{T}" /> from other pipelines.
    /// </remarks>
    [PublicAPI]
    public static Source<IAsyncConsumator<Carrier<T>>> MergeSources<T>(
        this FlowSource flowSource,
        params IAsyncConsumator<Carrier<T>>[] sources)
    {
        var processor = new MergeProcessor<T>(sources);
        return flowSource.On(processor);
    }
    
    // extension<T>(Source<IAsyncEnumerator<T>> left)
    // {
    //     /// <summary>
    //     ///     Races each upstream wait against a timeout, emitting either the element or an
    //     ///     <see cref="AwaitTimeout" /> marker when the source is idle too long.
    //     /// </summary>
    //     /// <param name="timeout">The maximum time to wait for an element before emitting a timeout marker.</param>
    //     /// <returns>A source emitting <see cref="Unio{T, AwaitTimeout}" /> items.</returns>
    //     [PublicAPI]
    //     public Source<IAsyncEnumerator<Unio<T, AwaitTimeout>>> Timeout(TimeSpan timeout)
    //     {
    //         return left.Next(new TimeoutProcessor<T>(timeout));
    //     }
    // }

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
}