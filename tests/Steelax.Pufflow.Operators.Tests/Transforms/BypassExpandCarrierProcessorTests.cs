using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Operators.Tests.Transforms;

/// <summary>
///     Black-box tests for the <c>Expand()</c> operator (backed by
///     <see cref="BypassExpandCarrierProcessor{T1,T2}" />): each value-bearing carrier expands into zero or more
///     carriers downstream (1:N), while bare-progress markers pass through so the stream watermark keeps advancing.
/// </summary>
public class BypassExpandCarrierProcessorTests
{
    private const int TimeoutMs = 2_000;

    private sealed record Snapshot(int? Value, long Watermark, bool HasValue);

    private static Snapshot Of(Carrier<int> item) =>
        item.HasValue ? new Snapshot(item.Value, item.Watermark, HasValue: true) : new Snapshot(null, item.Watermark, HasValue: false);

    /// <summary>Expands nothing — the input value produces no output elements.</summary>
    private static void ExpandNothing(scoped in int value, ExpandQueue<int> target) { }

    private static async Task<List<Snapshot>> RunPullAsync(
        IEnumerable<Carrier<int>> input,
        ExpandSelector<int, int> selector,
        FlowSource flow,
        CancellationToken cancellationToken)
    {
        flow
            .OnAsyncConsumatorSource(input)
            .Expand(selector)
            .Consume(out var reader);

        await flow.ExecuteAsync(cancellationToken);

        var result = new List<Snapshot>();
        await foreach (var item in reader.ReadAllAsync(TestContext.Current.CancellationToken))
            result.Add(Of(item));

        return result;
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task ExpandsEachValueIntoMany_InOrder_WithInheritedWatermarks()
    {
        var input = new Carrier<int>[]
        {
            new(1, Watermark.From(10)),
            new(2, Watermark.From(20)),
            new(3, Watermark.From(30)),
        };

        await using var flow = new FlowSource();
        var actual = await RunPullAsync(input, Double, flow, TestContext.Current.CancellationToken);

        Assert.Equal(
        [
            new Snapshot(1, 10, HasValue: true),
            new Snapshot(10, 10, HasValue: true),
            new Snapshot(2, 20, HasValue: true),
            new Snapshot(20, 20, HasValue: true),
            new Snapshot(3, 30, HasValue: true),
            new Snapshot(30, 30, HasValue: true),
        ], actual);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task EmptyExpansionWithWatermark_ReEmitsBareProgressCarrier()
    {
        // A value that expands into nothing still carries the stream's watermark: it is re-emitted as a bare
        // progress marker, so the downstream watermarks never stall (mirroring the filter operator's contract).
        var input = new Carrier<int>[]
        {
            new(1, Watermark.From(10)),
            new(2, Watermark.From(20)),
            new(3, Watermark.From(30)),
        };

        await using var flow = new FlowSource();
        var actual = await RunPullAsync(input, ExpandNothing, flow, TestContext.Current.CancellationToken);

        Assert.Equal(3, actual.Count);
        Assert.Equal(
        [
            new Snapshot(null, 10, HasValue: false),
            new Snapshot(null, 20, HasValue: false),
            new Snapshot(null, 30, HasValue: false),
        ], actual);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task EmptyExpansionWithoutWatermark_DropsTheInputValue()
    {
        // A value that expands into nothing and carries no watermark has nothing to contribute — it disappears.
        var input = new Carrier<int>[]
        {
            new(1, Watermark.Nothing),
            new(2, Watermark.Nothing),
            new(3, Watermark.Nothing),
        };

        await using var flow = new FlowSource();
        var actual = await RunPullAsync(input, ExpandNothing, flow, TestContext.Current.CancellationToken);

        Assert.Empty(actual);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task BareProgress_IsForwarded_AsAMarker()
    {
        var input = new Carrier<int>[]
        {
            new(1, Watermark.From(10)),
            new Carrier<int>(Watermark.From(50)),
            new(2, Watermark.From(100)),
        };

        await using var flow = new FlowSource();
        var actual = await RunPullAsync(input, Double, flow, TestContext.Current.CancellationToken);

        Assert.Equal(5, actual.Count);
        Assert.Equal(new Snapshot(null, 50, HasValue: false), actual[2]);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task BareProgressWithoutWatermark_IsDropped()
    {
        var input = new Carrier<int>[]
        {
            new(1, Watermark.From(10)),
            new Carrier<int>(Watermark.Nothing),
            new(2, Watermark.From(100)),
        };

        await using var flow = new FlowSource();
        var actual = await RunPullAsync(input, Double, flow, TestContext.Current.CancellationToken);

        Assert.Equal(4, actual.Count);
        Assert.Equal(new Snapshot(1, 10, HasValue: true), actual[0]);
        Assert.Equal(new Snapshot(10, 10, HasValue: true), actual[1]);
        Assert.Equal(new Snapshot(2, 100, HasValue: true), actual[2]);
        Assert.Equal(new Snapshot(20, 100, HasValue: true), actual[3]);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task SingleValue_ExpandsBeyondSmallScratch()
    {
        // One input expands into 100 outputs: the operator's internal queue grows as needed and nothing is lost.
        var input = new Carrier<int>[] { new(1, Watermark.From(42)) };

        await using var flow = new FlowSource();
        var actual = await RunPullAsync(input, ExpandHundred, flow, TestContext.Current.CancellationToken);

        Assert.Equal(100, actual.Count);
        Assert.All(actual, static s => Assert.Equal(42, s.Watermark));
        Assert.Equal(Enumerable.Range(100, 100).Select(static i => (int?)i), actual.Select(static s => s.Value));
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task EmptySource_YieldsNothing()
    {
        await using var flow = new FlowSource();
        var actual = await RunPullAsync([], Double, flow, TestContext.Current.CancellationToken);

        Assert.Empty(actual);
    }

    /// <summary>Expands a value into two: the value itself and its tenfold.</summary>
    private static void Double(scoped in int value, ExpandQueue<int> target)
    {
        target.Write(value);
        target.Write(value * 10);
    }

    /// <summary>Expands a value into a hundred consecutive values (larger than the initial queue capacity).</summary>
    private static void ExpandHundred(scoped in int value, ExpandQueue<int> target)
    {
        for (var i = 0; i < 100; i++)
            target.Write(value * 100 + i);
    }
}
