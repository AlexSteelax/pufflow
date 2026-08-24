using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Aggregators.Buffering;

/// <summary>
///     Black-box tests for the <c>Buffering(capacity)</c> operator (backed by
///     <see cref="BypassBufferProcessor{T}" />): a push source writes values through the operator and the
///     resulting pull stream (consumator) is observed downstream. The composite pipe
///     <c>Fuse(out IAsyncProducator, out IAsyncConsumator, ctx)</c> bridges the push side (writer) to the
///     pull side (reader) over a single bounded SPSC channel.
/// </summary>
public static class BypassBufferProcessorTests
{
    private const int TimeoutMs = 1_000;

    private static readonly MapSelector<int, int> TimesTen = static (scoped in int value) => value * 10;

    private static async Task<List<int>> RunAsync(IEnumerable<int> input, int capacity, FlowSource flow, CancellationToken cancellationToken)
    {
        flow
            .OnAsyncProducatorSource(input)
            .Buffering(capacity)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        return await reader.ReadAllAsync(TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);
    }

    public sealed class Buffering
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task FastSource_YieldsAllItemsInOrder()
        {
            // A small source passes through the buffer unchanged, in arrival order.
            await using var flow = new FlowSource();
            var results = await RunAsync([1, 2, 3, 4, 5], capacity: 2, flow, TestContext.Current.CancellationToken);

            Assert.Equal([1, 2, 3, 4, 5], results);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task EmptySource_YieldsNothing()
        {
            await using var flow = new FlowSource();
            var results = await RunAsync([], capacity: 2, flow, TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task SourceLargerThanBuffer_AllItemsDelivered()
        {
            // The writer blocks on the full buffer (backpressure) and resumes as the reader drains it.
            await using var flow = new FlowSource();
            var results = await RunAsync(Enumerable.Range(0, 100), capacity: 2, flow, TestContext.Current.CancellationToken);

            Assert.Equal(Enumerable.Range(0, 100), results);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task CapacityOfOne_SequentialDelivery()
        {
            // A capacity of one still delivers every item exactly once, in order.
            await using var flow = new FlowSource();
            var results = await RunAsync(Enumerable.Range(0, 20), capacity: 1, flow, TestContext.Current.CancellationToken);

            Assert.Equal(Enumerable.Range(0, 20), results);
        }
    }

    public sealed class PushThroughMap
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task SyncPushSource_ThroughMapPipe_IntoBuffer()
        {
            // Sync push source → sync push-push map pipe → composite buffer (push→pull) → async pull sink.
            // The map projects each value before it crosses the buffer bridge.
            await using var flow = new FlowSource();

            flow
                .OnProducatorSource([1, 2, 3, 4])
                .Map(TimesTen)
                .Buffering(capacity: 2)
                .Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken)
                .ToListAsync(TestContext.Current.CancellationToken);

            Assert.Equal([10, 20, 30, 40], results);
        }
    }
}
