using BenchmarkDotNet.Attributes;
using Steelax.Pufflow;
using Steelax.Pufflow.Operators;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Benchmarks;

/// <summary>
///     Compares the raw pipeline overhead against the cost of adding the <c>Chunking</c> operator. Both
///     benchmarks build a fresh pipeline per iteration (the sources are single-use), fill the channel with
///     <see cref="ItemsPerInvoke" /> records in <c>IterationSetup</c> (outside the measurement) and measure
///     only the processing time via <see cref="FlowSource.ExecuteAsync" />. The sink is a null sink
///     (<see cref="FlowNullSinkConsumator{T}" />) so no channel read is measured.
/// </summary>
[MemoryDiagnoser]
[IterationCount(7)]
[WarmupCount(3)]
public class PipelineBenchmarks
{
    private const int ItemsPerInvoke = 50_000_000;
    private const int ChunkSize = 512;
    private static readonly TimeSpan ChunkLinger = TimeSpan.FromMilliseconds(100);

    private FlowSource _flow = null!;

    /// <summary>Baseline: <c>source → null sink</c>, no operators. Measures the full pipeline overhead.</summary>
    [Benchmark(OperationsPerInvoke = ItemsPerInvoke)]
    public async Task Baseline()
    {
        await _flow.ExecuteAsync();
    }

    /// <summary>Chunking: <c>source → Chunking → null sink</c>. Adds the chunking operator on top.</summary>
    [Benchmark(OperationsPerInvoke = ItemsPerInvoke)]
    public async Task Chunking()
    {
        await _flow.ExecuteAsync();
    }

    /// <summary>Builds a fresh pipeline for the baseline benchmark (no operator).</summary>
    [IterationSetup(Target = nameof(Baseline))]
    public void SetupBaseline()
    {
        _flow = new FlowSource();
        _flow
            .OnAsyncConsumatorSource(Enumerable.Range(0, ItemsPerInvoke))
            .Consume();
    }

    /// <summary>Builds a fresh pipeline for the chunking benchmark.</summary>
    [IterationSetup(Target = nameof(Chunking))]
    public void SetupChunking()
    {
        _flow = new FlowSource();
        _flow
            .OnAsyncConsumatorSource(Enumerable.Range(0, ItemsPerInvoke))
            .Chunking(ChunkSize, ChunkLinger)
            // A dedicated chunk null sink: it disposes each Chunk<T>, returning the buffer to the pool.
            // The generic null sink cannot do this without boxing a struct.
            .End(new ChunkNullSink<int>());
    }

    /// <summary>Releases the pipeline (safe after the loop has already completed).</summary>
    [IterationCleanup]
    public void Cleanup()
    {
        _flow.Dispose();
    }
}
