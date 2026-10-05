using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Transforms;

/// <summary>
///     Black-box tests for <c>Filter</c> over <see cref="Carrier{T}" /> streams. The filter drops the values that
///     fail the predicate, but dropping a value must not stall the stream's watermark progress: a rejected carrier
///     that carries a real watermark is re-emitted as a bare progress carrier, so downstream watermark consumers
///     always observe the full, non-decreasing progress sequence.
/// </summary>
public class FilterCarrierProcessorTests
{
    private const int TimeoutMs = 2_000;

    private sealed record Snapshot(int? Value, long Watermark, bool HasValue);

    private static Snapshot Of(Carrier<int> item) =>
        item.HasValue ? new Snapshot(item.Value, item.Watermark, HasValue: true) : new Snapshot(null, item.Watermark, HasValue: false);

    private static async Task<List<Snapshot>> RunPullAsync(IEnumerable<Carrier<int>> input, FilterPredicate<int> predicate, FlowSource flow, CancellationToken cancellationToken)
    {
        flow
            .OnAsyncConsumatorSource(input)
            .Filter(predicate)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        var result = new List<Snapshot>();
        await foreach (var item in reader.ReadAllAsync(TestContext.Current.CancellationToken))
            result.Add(Of(item));

        return result;
    }

    private static async Task<List<Snapshot>> RunPushAsync(IEnumerable<Carrier<int>> input, FilterPredicate<int> predicate, FlowSource flow, CancellationToken cancellationToken)
    {
        flow
            .OnAsyncProducatorSource(input)
            .Filter(predicate)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        var result = new List<Snapshot>();
        await foreach (var item in reader.ReadAllAsync(TestContext.Current.CancellationToken))
            result.Add(Of(item));

        return result;
    }

    public sealed class Pull
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task KeptCarriers_PassThrough_OrderAndWatermarksPreserved()
        {
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new(2, Watermark.From(20)),
                new Carrier<int>(Watermark.From(30)),
                new(3, Watermark.From(40)),
            };

            var expected = input.Select(static i => Of(i)).ToList();

            await using var flow = new FlowSource();
            var actual = await RunPullAsync(input, static (scoped in int v) => true, flow, TestContext.Current.CancellationToken);

            Assert.Equal(expected, actual);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task BareProgress_AlwaysForwarded_RegardlessOfPredicate()
        {
            // Bare progress carriers carry no value to evaluate and are structural: they must pass even when the
            // predicate rejects every data value.
            var input = new[]
            {
                new Carrier<int>(Watermark.From(50)),
                new Carrier<int>(Watermark.From(60)),
            };

            await using var flow = new FlowSource();
            var actual = await RunPullAsync(input, static (scoped in int v) => false, flow, TestContext.Current.CancellationToken);

            Assert.Equal(2, actual.Count);
            Assert.Equal(new Snapshot(null, 50, HasValue: false), actual[0]);
            Assert.Equal(new Snapshot(null, 60, HasValue: false), actual[1]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task RejectedWatermarkedValue_ReEmitsBareProgress_CarryingItsWatermark()
        {
            // The middle value is rejected, but its watermark (100) belongs to the stream's progress and must not
            // be swallowed: the filter forwards it as a bare progress carrier so the watermark never stalls.
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new(2, Watermark.From(100)),
                new(3, Watermark.From(200)),
            };

            await using var flow = new FlowSource();
            var actual = await RunPullAsync(input, static (scoped in int v) => v != 2, flow, TestContext.Current.CancellationToken);

            Assert.Equal(3, actual.Count);
            Assert.Equal(new Snapshot(1, 10, HasValue: true), actual[0]);
            Assert.Equal(new Snapshot(null, 100, HasValue: false), actual[1]);
            Assert.Equal(new Snapshot(3, 200, HasValue: true), actual[2]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task RejectedCarrierWithoutWatermark_IsFullyDropped()
        {
            // Dropping a carrier that carries no watermark must not fabricate an (empty) progress item.
            var input = new[]
            {
                new Carrier<int>(1, Watermark.Nothing()),
                new Carrier<int>(2, Watermark.Nothing()),
            };

            await using var flow = new FlowSource();
            var actual = await RunPullAsync(input, static (scoped in int v) => v == 1, flow, TestContext.Current.CancellationToken);

            var item = Assert.Single(actual);
            Assert.Equal(new Snapshot(1, -1, HasValue: true), item);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task DroppedHighestWatermark_DoesNotFreezeProgress()
        {
            // The only rejected value carries the stream maximum: without re-emission the downstream watermark
            // would freeze at 10. The re-emitted bare progress must carry the maximum through.
            var input = new[]
            {
                new Carrier<int>(1, Watermark.From(10)),
                new Carrier<int>(2, Watermark.From(100)),
            };

            await using var flow = new FlowSource();
            var actual = await RunPullAsync(input, static (scoped in int v) => v == 1, flow, TestContext.Current.CancellationToken);

            Assert.Equal(2, actual.Count);
            Assert.Equal(new Snapshot(1, 10, HasValue: true), actual[0]);
            Assert.Equal(new Snapshot(null, 100, HasValue: false), actual[1]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task MixedDrops_WatermarkSequence_StaysNonDecreasing()
        {
            // A monotonic input sliced by arbitrary drops: real watermarks in the output may not go backwards.
            const int n = 50;

            var input = Enumerable.Range(0, n)
                .Select(i => new Carrier<int>(i, Watermark.From((i + 1) * 10 / 3)))
                .ToArray();

            await using var flow = new FlowSource();
            var actual = await RunPullAsync(input, static (scoped in int v) => v % 3 != 1, flow, TestContext.Current.CancellationToken);

            var droppedCount = Enumerable.Range(0, n).Count(static i => i % 3 == 1);

            // Values: only the ones that pass.
            var values = actual.Where(static s => s.HasValue).Select(static s => s.Value).ToArray();
            Assert.Equal(Enumerable.Range(0, n).Where(i => i % 3 != 1).Select(i => (int?)i).ToArray(), values);

            // Progress: kept carriers + re-emitted bare carriers form the full non-decreasing watermark sequence,
            // closing on the input maximum.
            var watermarks = actual.Select(static s => s.Watermark).Where(static w => w != -1).ToArray();
            Assert.Equal(n - droppedCount + droppedCount, watermarks.Length);
            Assert.OrderIncreasing(watermarks, false);
            Assert.Equal(Watermark.From(n * 10 / 3), watermarks.Max());
        }
    }

    public sealed class Push
    {
        [Fact(Timeout = TimeoutMs)]
        public async Task RejectedWatermarkedValue_ReEmitsBareProgress_CarryingItsWatermark()
        {
            // The push (producator) endpoint must implement the same progress-preserving contract.
            var input = new Carrier<int>[]
            {
                new(1, Watermark.From(10)),
                new(2, Watermark.From(100)),
                new(3, Watermark.From(200)),
            };

            await using var flow = new FlowSource();
            var actual = await RunPushAsync(input, static (scoped in int v) => v != 2, flow, TestContext.Current.CancellationToken);

            Assert.Equal(3, actual.Count);
            Assert.Equal(new Snapshot(1, 10, HasValue: true), actual[0]);
            Assert.Equal(new Snapshot(null, 100, HasValue: false), actual[1]);
            Assert.Equal(new Snapshot(3, 200, HasValue: true), actual[2]);
        }

        [Fact(Timeout = TimeoutMs)]
        public async Task RejectedCarrierWithoutWatermark_IsFullyDropped()
        {
            var input = new[]
            {
                new Carrier<int>(1, Watermark.Nothing()),
                new Carrier<int>(2, Watermark.Nothing()),
            };

            await using var flow = new FlowSource();
            var actual = await RunPushAsync(input, static (scoped in int v) => v == 1, flow, TestContext.Current.CancellationToken);

            var item = Assert.Single(actual);
            Assert.Equal(new Snapshot(1, -1, HasValue: true), item);
        }
    }
}