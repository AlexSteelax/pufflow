using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

public partial class TestPipeExtensionsTests
{
    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncProducatorSource_ToAsyncProducator()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncProducatorSource(Input)
            .ToAsyncProducator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task ProducatorSource_ToAsyncProducator()
    {
        await using var flow = new FlowSource();

        flow
            .OnProducatorSource(Input)
            .ToAsyncProducator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncConsumatorSource_ToAsyncProducator()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(Input)
            .ToAsyncProducator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task ConsumatorSource_ToAsyncProducator()
    {
        await using var flow = new FlowSource();

        flow
            .OnConsumatorSource(Input)
            .ToAsyncProducator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }
}
