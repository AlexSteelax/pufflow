using System.Threading.Channels;
using Unio;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

/// <summary>
///     Alternating transition chains: blocks of different families (sync/async push and pull,
///     composite push→pull bridges and hybrid pull→push pipes) are chained back to back so the
///     resolved pipeline crosses every transition. Four to five blocks per chain; the remaining
///     tests are variations of the same pattern.
/// </summary>
public class TransitionChainTests
{
    private const int TimeoutMs = 1_000;

    private static readonly int[] Input = [1, 2, 3];

    private static readonly int[] Expected = [30, 50, 70];

    private static int Id(int value) => value;

    private readonly record struct DummyWatermark;

    private static Unio<int, DummyWatermark> TimesTenInUnio(Unio<int, DummyWatermark> value)
    {
        return value.TryPickT0(out var v, out _) ? v * 10 : value;
    }

    private static async Task<List<int>> DrainAsync(FlowSource flow, ChannelReader<int> reader, CancellationToken cancellationToken)
    {
        await flow.ExecuteAsync(cancellationToken);
        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task SyncPushSource_PushPushChain_Composite_PullSink()
    {
        // Sync push source → sync push-push → async push-push → sync push-push → composite (push→pull)
        // → async pull sink.
        await using var flow = new FlowSource();

        flow
            .OnProducatorSource(Input)
            .ToProducator(static v => v * 2)
            .ToAsyncProducator(static v => v + 1)
            .ToProducator(static v => v * 10)
            .ToAsyncConsumator(static v => v)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncPushSource_PushPush_Composite_Hybrid_PushPush_PushSink()
    {
        // Async push source → async push-push → composite (push→pull) → hybrid (pull→push) → sync
        // push-push → sync push sink. Crosses the push↔pull boundary twice.
        await using var flow = new FlowSource();

        flow
            .OnAsyncProducatorSource(Input)
            .ToAsyncProducator(static v => v * 2)
            .ToAsyncConsumator(static v => v + 1)
            .ToAsyncProducator(static v => v * 10)
            .ToProducator(static v => v)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task SyncConsumatorSource_Hybrid_PushPushChain_PushSink()
    {
        // Sync pull source → hybrid (pull→push) → sync push-push → async push-push → async push sink.
        await using var flow = new FlowSource();

        flow
            .OnConsumatorSource(Input)
            .ToAsyncProducator(static v => v * 2)
            .ToProducator(static v => v + 1)
            .ToAsyncProducator(static v => v * 10)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncConsumatorSource_PullPush_Hybrid_PushPush_PushSink()
    {
        // Async pull source → sync pull-pull → hybrid (pull→push) → async push-push → async push sink.
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(Input)
            .ToConsumator(static v => v * 2)
            .ToAsyncProducator(static v => v + 1)
            .ToProducator(static v => v * 10)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task SyncConsumatorSource_PullPullChain_PullSink()
    {
        // Sync pull source → sync→async pull-pull → async→sync pull-pull → sync→async pull-pull →
        // async pull sink (a pure pull chain, resolved via merge).
        await using var flow = new FlowSource();

        flow
            .OnConsumatorSource(Input)
            .ToAsyncConsumator(static v => v * 2)
            .ToConsumator(static v => v + 1)
            .ToAsyncConsumator(static v => v * 10)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncConsumatorSource_PullPullChain_PullSink()
    {
        // Async pull source → async pull-pull → async→sync pull-pull → sync→async pull-pull → async
        // pull sink (a pure pull chain, resolved via merge).
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(Input)
            .ToAsyncConsumator(static v => v * 2)
            .ToConsumator(static v => v + 1)
            .ToAsyncConsumator(static v => v * 10)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }
[Fact(Timeout = 5_000)]
    public async Task ThirtyBlocks_AlternatingPullPushTransitions()
    {
        // 30 blocks alternating between families: a pull chain of 14 pull-pull pipes (sync/async),
        // one hybrid pull→push transition, then a push chain of 15 push-push pipes (sync/async).
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(Input)
            // Pull chain (merge): 14 pull-pull blocks alternating sync/async.
            .ToConsumator(static v => v * 2)
            .ToAsyncConsumator(static v => v + 1)
            .ToConsumator(Id)
            .ToAsyncConsumator(Id)
            .ToConsumator(Id)
            .ToAsyncConsumator(Id)
            .ToConsumator(Id)
            .ToAsyncConsumator(Id)
            .ToConsumator(Id)
            .ToAsyncConsumator(Id)
            .ToConsumator(Id)
            .ToAsyncConsumator(Id)
            .ToConsumator(Id)
            .ToAsyncConsumator(Id)
            // Pull → push transition (collection): the hybrid pipe.
            .ToAsyncProducator(static v => v * 10)
            // Push chain (collection): 15 push-push blocks alternating sync/async.
            .ToProducator(Id)
            .ToAsyncProducator(Id)
            .ToProducator(Id)
            .ToAsyncProducator(Id)
            .ToProducator(Id)
            .ToAsyncProducator(Id)
            .ToProducator(Id)
            .ToAsyncProducator(Id)
            .ToProducator(Id)
            .ToAsyncProducator(Id)
            .ToProducator(Id)
            .ToAsyncProducator(Id)
            .ToProducator(Id)
            .ToAsyncProducator(Id)
            .ToProducator(Id)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }
[Fact(Timeout = 5_000)]
    public async Task SyncPushSource_WatermarkedToUnioWithDummyWatermark()
    {
        // Sync push source → sync push-push → async push-push → composite (push→pull) → hybrid
        // (pull→push, the transition into Unio<int, DummyWatermark>) → async push-push → async
        // push-push → composite (push→pull, unpacking the value) → async pull sink. The watermark is a
        // dummy placeholder type; the final stage emits the plain value, not a watermarked wrapper.
        await using var flow = new FlowSource();

        flow
            .OnProducatorSource(Input)
            .ToProducator(static v => v * 2)
            .ToAsyncProducator(static v => v + 1)
            .ToAsyncConsumator(static v => v)
            .ToAsyncProducator(static v => (Unio<int, DummyWatermark>)v)
            .ToAsyncProducator(TimesTenInUnio)
            .ToAsyncProducator(static u => u)
            .ToAsyncConsumator(static u => u.AsT0)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }
}
