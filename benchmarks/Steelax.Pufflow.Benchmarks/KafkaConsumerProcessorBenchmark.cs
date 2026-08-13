using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Steelax.Pufflow;
using Steelax.Pufflow.Operators.Kafka;
using Steelax.Pufflow.Sdk.Test;
using Steelax.Pufflow.Sdk.Test.Kafka;

namespace Steelax.Pufflow.Benchmarks;

/// <summary>
///     Benchmarks the <see cref="KafkaConsumerProcessor{TKey,TValue}" /> consume loop over a
///     <see cref="TestConsumer{TKey,TValue}" /> (no real broker). The channel is pre-filled with
///     <see cref="ItemsPerInvoke" /> records in <c>IterationSetup</c> (outside the measurement), and the
///     benchmark itself only waits for the pipeline to process them all and stop cleanly via the
///     end-of-stream abort. The measured time is therefore the pure processor throughput.
/// </summary>
[MemoryDiagnoser]
[IterationCount(7)]
[WarmupCount(3)]
public class KafkaConsumerProcessorBenchmark
{
    private const int ItemsPerInvoke = 30_000_000;

    private FlowSource _flow = null!;

    [BenchmarkCancellation]
    public CancellationToken CancellationToken { get; set; }

    /// <summary>Builds a fresh Kafka pipeline and pre-fills the channel with the records to process.</summary>
    /// <remarks>
    ///     The processor is single-use: after the channel is drained the loop aborts. Hence a new pipeline
    ///     is built per iteration, but all construction and channel fills happen outside the measurement.
    /// </remarks>
    [IterationSetup]
    public void Setup()
    {
        var options = new KafkaConsumerOptions(TimeSpan.FromMilliseconds(100))
        {
            EmergencyRatio = 0.1f,
            IdleRatio = 0.1f,
            WindowSize = 4,
            WindowLifetime = TimeSpan.FromMilliseconds(250),
            EmergencyCapacity = 16,
            AdvanceStrategy = AdvanceStrategy.ManualCommit
        };

        // Pre-fill the channel, then complete it: TestConsumer throws TestChannelCompletedException once the
        // channel is drained → TestKafkaErrorPolicy → KafkaErrorAction.Abort → clean loop exit.
        var items = Enumerable
            .Range(0, ItemsPerInvoke)
            .Select(static i => new KeyValuePair<string, string>(i.ToString(), "value"));
        
        _flow = new FlowSource(CancellationToken);
        _flow
            .OnKafkaSource(options, items)
            .Consume();
    }

    /// <summary>
    ///     Starts the consume loop and waits for it to finish processing all pre-filled records. The elapsed
    ///     time covers the whole consume → window → flush cycle for <see cref="ItemsPerInvoke" /> records.
    /// </summary>
    [Benchmark(OperationsPerInvoke = ItemsPerInvoke)]
    public async Task Consume()
    {
        await _flow.ExecuteAsync();
    }

    /// <summary>Releases the pipeline (safe after the loop has already completed).</summary>
    [IterationCleanup]
    public void Cleanup()
    {
        _flow.Dispose();
    }
}
