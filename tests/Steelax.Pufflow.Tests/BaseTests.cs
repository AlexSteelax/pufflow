using Steelax.Pufflow.Sdk.Test;

namespace Steelax.Pufflow.Tests;

public class BaseTests
{
    [Fact]
    public async Task AsyncConsumatorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        var sourceFlow = source
            .OnAsyncConsumatorSource(Enumerable.Range(1, 5))
            .ToAsyncConsumator(static v => v)
            .ToAsyncConsumator(static v => v)
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5), ret);
    }

    [Fact]
    public async Task AsyncProducatorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        var sourceFlow = source
            .OnAsyncProducatorSource(Enumerable.Range(1, 5))
            .ToAsyncProducator(static v => v)
            .ToAsyncProducator(static v => v)
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5), ret);
    }

    [Fact]
    public async Task ProducatorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        source
            .OnProducatorSource(Enumerable.Range(1, 5))
            .ToProducator(static v => v)
            .ToProducator(static v => v)
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(1, 5), ret);
    }

    [Fact]
    public async Task ProducatorToAsyncConsumatorChain_ProducesExpectedResults()
    {
        await using var source = new FlowSource();

        source
            .OnProducatorSource(Enumerable.Range(1, 5))
            .ToAsyncConsumator(static v => v * 2)
            .Consume(out var reader);

        await source.ExecuteAsync(TestContext.Current.CancellationToken);

        var ret = await reader.ReadAllAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal([2, 4, 6, 8, 10], ret);
    }
}
