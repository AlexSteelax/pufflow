using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    extension<T>(Source<IAsyncConsumator<Carrier<T>>> left)
    {
        /// <summary>
        ///     Groups consecutive carried values into <see cref="Carrier{ChunkOfT}" /> items: a chunk's payload is the
        ///     accumulated <see cref="Chunk{T}" /> of the raw values, and the surrounding <see cref="Carrier{T}" />
        ///     watermark is the maximum watermark seen across the window (including any bare progress items). Windows
        ///     close on the requested element size, after <paramref name="linger" />, or at end-of-stream.
        /// </summary>
        /// <param name="minimumSize">The minimum number of carried values per chunk.</param>
        /// <param name="linger">The maximum time to wait for a partial chunk before emitting it.</param>
        /// <param name="strategy">The buffer-capacity strategy used to size each chunk.</param>
        /// <returns>A source emitting <see cref="Carrier{ChunkOfT}" /> items.</returns>
        [PublicAPI]
        public Source<IAsyncConsumator<Carrier<Chunk<T>>>> Chunking(
            int minimumSize,
            TimeSpan linger,
            ChunkCapacityStrategy strategy = ChunkCapacityStrategy.Exact)
        {
            var chunker = new Chunker<T>(strategy);
            var processor = new CarrierChunkProcessor<T>(chunker, minimumSize, linger);
            return left.Next(processor);
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
}
