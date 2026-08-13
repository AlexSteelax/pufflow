using Steelax.Pufflow;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Operators.Aggregators.Chunking;

namespace Steelax.Pufflow.Benchmarks;

/// <summary>
///     A null sink for <see cref="Chunk{T}" /> streams: reads every chunk and disposes it, returning the
///     underlying buffer to the pool. Unlike the generic <see cref="FlowNullSinkConsumator{T}" /> (which
///     cannot dispose without boxing a struct), the element type is known statically, so <c>using</c> on
///     the <see cref="Chunk{T}" /> struct is a zero-alloc constrained call.
/// </summary>
[Flow]
internal partial class ChunkNullSink<T>
{
    public void Fuse(IAsyncConsumator<Chunk<T>> source, FlowContext context)
    {
        context.RegisterBackground(() => ConsumeLoopAsync(source, context));
    }

    private static async Task ConsumeLoopAsync(IAsyncConsumator<Chunk<T>> source, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (source.TryRead(out var chunk))
            {
                using (chunk) { }
                continue;
            }

            if (!await source.WaitToReadAsync())
                break;
        }
    }
}
