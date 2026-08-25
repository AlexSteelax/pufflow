using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

public partial class TestPipeExtensionsTests
{
    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncProducatorSource_ToConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncProducatorSource(Input)
            .ToConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task ProducatorSource_ToConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnProducatorSource(Input)
            .ToConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task AsyncConsumatorSource_ToConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnAsyncConsumatorSource(Input)
            .ToConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }

    [Fact(Timeout = TimeoutMs)]
    public async Task ConsumatorSource_ToConsumator()
    {
        await using var flow = new FlowSource();

        flow
            .OnConsumatorSource(Input)
            .ToConsumator(TimesTen)
            .Consume(out var reader);

        var results = await DrainAsync(flow, reader, TestContext.Current.CancellationToken);
        Assert.Equal(Expected, results);
    }
}
