using System.Threading.Channels;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

/// <summary>
///     Unit tests for the <c>Buffering(capacity)</c> operator (backed by the ctor-based
///     <see cref="BypassBufferAProdToACons{T}" /> / <see cref="BypassBufferProdToACons{T}" /> composite
///     blocks): a push source feeds the operator and the resulting pull stream is observed downstream over
///     a single bounded SPSC channel.
/// </summary>
public static partial class BufferProcessorTests
{
    /// <summary>Builds a push pipeline through <c>Buffering(capacity)</c>, returning the flow, the source writer and the reader.</summary>
    private static (FlowSource Flow, ChannelWriter<int> Writer, ChannelReader<int> Reader) BuildPipeline(int capacity)
    {
        var flow = new FlowSource();

        flow
            .OnAsyncProducatorSource<int>(out var writer)
            .Buffering(capacity)
            .Consume(out var reader);

        return (flow, writer, reader);
    }

    private static async Task<List<int>> CollectAsync(int capacity, IEnumerable<int> initial)
    {
        var (flow, writer, reader) = BuildPipeline(capacity);

        foreach (var item in initial)
            writer.TryWrite(item);

        writer.TryComplete();

        await using var _ = flow;
        await flow.ExecuteAsync(TestContext.Current.CancellationToken);

        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }
}