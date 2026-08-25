using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Aggregators.Warming;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;
using Unio;

namespace Steelax.Pufflow.Operators.Tests.Pipelines;

/// <summary>
///     Mirrors the production <c>AnalogProcessor</c> pipeline: Kafka → SkipNoSignal → Map →
///     Buffering → Warming → Map → Map → Buffering → sink. In flow terms it is
///     async push source → push-push → push-push → composite (push→pull) → hybrid (pull→push) →
///     push-push → push-push → composite (push→pull) → pull sink — two buffer bridges with a warming
///     hybrid between them.
/// </summary>
public class ComplexPipelineTests
{
    private const int TimeoutMs = 1_000;

    private static readonly Watermarked<int>[] Input =
    [
        new(1, Watermark.From(1)),
        new(2, Watermark.From(2)),
        new(3, Watermark.From(3))
    ];

    private static readonly string[] Expected =
    [
        "4",
        "6",
        "8"
    ];

    private static readonly WarmOptions WarmOptions = new()
    {
        MaxConcurrency = 4,
        MaxQueued = 16,
        QueueWeightLimit = 10240,
        SegmentCapacity = 256,
        SegmentLinger = TimeSpan.FromSeconds(2)
    };

    private static readonly MapSelector<int, int> IdentityKey = static (scoped in int value) => value;

    [Fact(Timeout = TimeoutMs)]
    public async Task KafkaLikePipeline_FlowsThroughTwoBuffersAndWarming()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncProducatorSource(Input)
            .Map(static (scoped in w) => new Watermarked<int>(w.Value + 1, w.Watermark))
            .Map(static (scoped in w) => new Watermarked<int>(w.Value * 2, w.Watermark))
            .Buffering(128)
            .Warming(WarmOptions, new StubJobFactory(), IdentityKey, new NoWarmPolicy(), new QueueAccumulatorFactory())
            .Watermarked()
            .Map(static (scoped in w) => new Watermarked<string>(w.Value.ToString(), w.Watermark))
            .Map(static (scoped in w) => w)
            .Buffering(256)
            .Consume(out var reader);

        await flow.ExecuteAsync(TestContext.Current.CancellationToken);
        var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Expected, results.Select(static w => w.Value));
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task SyncKafkaLikePipeline_FlowsThroughTwoBuffersAndWarming()
    {
        // Mirrors the production AnalogProcessor exactly: OnKafkaSource is a SYNC push source
        // (IProducator<Watermarked<...>>), so the first segment (source → sync push-push → sync
        // push-push → sync composite) is synchronous, while the second segment (hybrid → async
        // push-push → async push-push → async composite) is asynchronous. The two segments are
        // separated by the pull→push hybrid and must not be mixed when the chain is resolved.
        await using var flow = new FlowSource();

        flow
            .OnProducatorSource(Input.Select(static w => w.Value))
            .Map(static (scoped in int v) => new Watermarked<int>(v, Watermark.Nothing()))
            .Map(static (scoped in Watermarked<int> w) => new Watermarked<int>(w.Value + 1, w.Watermark))
            .Buffering(128)
            .Warming(WarmOptions, new StubJobFactory(), IdentityKey, new NoWarmPolicy(), new QueueAccumulatorFactory())
            .Map(static (scoped in Unio<int, Watermark> u) => u)
            .Map(static (scoped in Unio<int, Watermark> u) => u)
            .Buffering(256)
            .Consume(out var reader);

        await flow.ExecuteAsync(TestContext.Current.CancellationToken);
        var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([2, 3, 4], results.Select(static u => u.AsT0));
    }

    /// <summary>Warms nothing: every key is a passthrough.</summary>
    private sealed class NoWarmPolicy : IWarmPolicy<int, int>
    {
        public bool ShouldWarm(int key) => false;

        public void OnWarmed(int key, int warm)
        {
        }
    }

    /// <summary>Creates warming jobs that complete immediately and produce no warm data.</summary>
    private sealed class StubJobFactory : IJobFactory<int, int>
    {
        public IAsyncJob<int, int> CreateAsyncJob() => new StubJob();
    }

    /// <summary>An immediately-completing warming job with no results.</summary>
    private sealed class StubJob : IAsyncJob<int, int>
    {
        public Task ExecuteAsync(int[] keys, CancellationToken cancellationToken) => Task.CompletedTask;

        public ReadOnlySpan<KeyValuePair<int, int>> GetResult() => [];

        public void Dispose()
        {
        }
    }

    /// <summary>Creates per-key accumulators that release each stored value as its own group.</summary>
    private sealed class QueueAccumulatorFactory : IWarmAccumulatorFactory<int, int>
    {
        public WarmAccumulator<int, int> Create(int key) => new QueueAccumulator();
    }

    private sealed class QueueAccumulator : WarmAccumulator<int, int>
    {
        private readonly Queue<int> _items = new();

        protected internal override int EstimatedWeight => 1;

        protected override void Add(int value) => _items.Enqueue(value);

        protected override bool TryConsume(out int group, out int weight)
        {
            if (_items.TryDequeue(out var value))
            {
                group = value;
                weight = 1;
                return true;
            }

            group = default;
            weight = 0;
            return false;
        }
    }
}
