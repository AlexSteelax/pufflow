using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

public partial class TestPipeExtensionsTests
{
    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncProducatorSource_ToAsyncConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncProducatorSource(Input)
            .ToAsyncConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task ProducatorSource_ToAsyncConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnProducatorSource(Input)
            .ToAsyncConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncConsumatorSource_ToAsyncConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(Input)
            .ToAsyncConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task ConsumatorSource_ToAsyncConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnConsumatorSource(Input)
            .ToAsyncConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }
}
