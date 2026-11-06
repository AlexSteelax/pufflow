using Steelax.Pufflow.Operators.Aggregators;
#pragma warning disable CS1591
using Steelax.Pufflow.Operators.Aggregators.Chunking;
using Steelax.Pufflow.Operators.Common;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    extension<T>(Fuse<IAsyncConsumator<Carrier<T>>> left)
    {
        [PublicAPI]
        public Fuse<IAsyncConsumator<Carrier<Chunk<T>>>> Chunking(
            int minimumSize,
            TimeSpan linger,
            ChunkCapacityStrategy strategy = ChunkCapacityStrategy.Exact)
        {
            var chunker = new CarrierChunker<T>(minimumSize, strategy);
            return left.Next(ChunkProcessor<Carrier<T>, Carrier<Chunk<T>>>.Configure(chunker, linger, null));
        }
    }

    extension<T>(Fuse<IAsyncConsumator<T>> left)
    {
        [PublicAPI]
        public Fuse<IAsyncConsumator<Chunk<T>>> Chunking(int minimumSize, TimeSpan linger, ChunkCapacityStrategy strategy = ChunkCapacityStrategy.Exact)
        {
            var chunker = new Chunker<T>(minimumSize, strategy);
            return left.Next(ChunkProcessor<T, Chunk<T>>.Configure(chunker, linger, null));
        }
    }
}