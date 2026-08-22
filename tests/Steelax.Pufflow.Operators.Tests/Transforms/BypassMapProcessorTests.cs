using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Transforms;

/// <summary>
///     Black-box tests for the <c>Map()</c> operator (backed by <see cref="BypassMapProcessor{T1,T2}" />): a
///     push source writes values through the operator and the projected push stream is observed downstream.
///     The pipe projects each element 1:1 and retains the projected value in a single hold-slot when the
///     downstream target is full (backpressure), so the selector is never re-invoked and order is preserved.
/// </summary>
public static class BypassMapProcessorTests
{
    private const int TimeoutMs = 1_000;

    private static readonly MapSelector<int, int> TimesTen = static (scoped in int value) => value * 10;
    private static readonly MapSelector<int, int> Increment = static (scoped in int value) => value + 1;
    private static readonly MapSelector<int, int> Identity = static (scoped in int value) => value;

    private static async Task<List<int>> RunAsync(
        IEnumerable<int> input,
        MapSelector<int, int> selector,
        FlowSource flow,
        CancellationToken cancellationToken)
    {
        flow
            .OnAsyncProducatorSource(input)
            .Map(selector)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        return await reader.ReadAllAsync(cancellationToken).ToListAsync(cancellationToken);
    }

    public sealed class Mapping
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task ProjectsEachElementInOrder()
        {
            // 1:1 projection, order preserved.
            await using var flow = new FlowSource();
            var results = await RunAsync([1, 2, 3, 4, 5], TimesTen, flow, TestContext.Current.CancellationToken);

            Assert.Equal([10, 20, 30, 40, 50], results);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task EmptySource_YieldsNothing()
        {
            await using var flow = new FlowSource();
            var results = await RunAsync([], Identity, flow, TestContext.Current.CancellationToken);

            Assert.Empty(results);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task LargeSource_AllItemsProjectedWithoutLoss()
        {
            // A large source exercises the hold-slot: when the downstream consumer is slower, the projected
            // value is retained and flushed first, so nothing is lost and order is kept.
            await using var flow = new FlowSource();
            var results = await RunAsync(Enumerable.Range(0, 100), Increment, flow, TestContext.Current.CancellationToken);

            Assert.Equal(Enumerable.Range(1, 100), results);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task ChainedSources_ProjectionComposes()
        {
            // Two Map stages compose: first * 10, then + 1 → 10x+1 for each element.
            await using var flow = new FlowSource();

            flow
                .OnAsyncProducatorSource([1, 2, 3])
                .Map(TimesTen)
                .Map(Increment)
                .Consume(out var reader);

            await flow.ExecuteAsync(TestContext.Current.CancellationToken);
            var results = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

            Assert.Equal([11, 21, 31], results);
        }
    }
}
