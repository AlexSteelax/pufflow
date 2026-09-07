using Steelax.Pufflow.Operators.Aggregators;
using Steelax.Pufflow.Operators.Aggregators.Buffering;
using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators;

/// <summary>
///     Provides extension operators that attach processing stages to a dataflow source.
/// </summary>
[PublicAPI]
public static partial class OperatorExtensions
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